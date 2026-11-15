//go:build e2e

// TestHA3 is what one bare-metal system node can't show, on real Linodes: three system nodes (etcd)
// and two docker workers, under the rules of 2026-11-14 (design notebook §0.7, §0.8): a worker that
// loses the control plane keeps its last desired state and stops nothing but the silos that asked
// (onControlPlaneLoss: Stop, after their after); only those, with onHostLoss: Recreate, are started
// elsewhere, and never twice; starting a cluster is explicit (kvoadm init); a worker holds when a
// control plane is older than what it last saw, but a deliberate restore is a new history.
//
// One cluster of five nanodes, made once; each part is a subtest that can run alone, and leaves
// the cluster as it found it (all five Ready, three voters, nothing cut, its namespace gone):
//
//	KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -v -run 'TestHA3$' -timeout 240m ./test/e2e/          # all six
//	KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -v -run 'TestHA3/cut' -timeout 90m ./test/e2e/       # one
//
// leader (a), cut (b), rejoin (c), upgrade (d), wipe (e), restore (f): the order they run in when
// all run (restore last: it resets the system nodes). KEVASO_E2E_STATE=DIR keeps the cluster for the
// next run (TestSweep with KEVASO_E2E_SWEEP=kvt-ha3- deletes it). Everything is labelled and tagged
// kvt-ha3-RUN and swept at the end.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Core's numbers the tests reason with (core's internal packages can't be imported here; each is
// named after where it lives).
const (
	coreHeartbeat = 10 * time.Second // internal/worker.DefaultHeartbeat
	coreHostLease = 40 * time.Second // internal/controlplane: Config.HostLease
	// internal/controllers/silo.StopMargin(after) for an after under 6 s × 1000: 15 s + after/1000, rounded up.
	coreFenceSlack = 6 * time.Second // internal/fence.WorstStopped: after + CheckEvery (1 s) + KillWithin (5 s)
	haVersion      = "v0.17.0"       // what the cluster starts at; upgrade moves it a patch on
)

func stopMargin(after time.Duration) time.Duration {
	m := 15*time.Second + after/1000
	return (m + time.Second - 1).Truncate(time.Second)
}

var cps = []string{"cp1", "cp2", "cp3"}

// ha3 is the five-machine cluster.
type ha3 struct {
	t    *testing.T // the parent test
	ctx  context.Context
	b    *bed
	k    *kit
	c    *cluster
	ms   map[string]machine
	ssh  []string
	recv string // where the recovery key is kept (0600, never logged)

	mu    sync.Mutex
	built map[string]string // version → kevasod
}

func TestHA3(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 235*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-ha3-")
	t.Logf("run %s, region %s", b.Tag, b.Region)
	b.CleanupAtEnd(t)
	h := newHA3(t, ctx, b)
	for _, part := range []struct {
		name string
		run  func(*testing.T, *ha3)
	}{{"leader", ha3Leader}, {"cut", ha3Cut}, {"rejoin", ha3Rejoin}, {"upgrade", ha3Upgrade}, {"wipe", ha3Wipe}, {"restore", ha3Restore}} {
		t.Run(part.name, func(t *testing.T) {
			start := time.Now()
			h.healthy(t)
			h.clearLeftovers(t)
			part.run(t, h)
			t.Logf("── %s: %s ──", part.name, time.Since(start).Round(time.Second))
		})
	}
}

// newHA3 makes the cluster (or takes over the one kept in KEVASO_E2E_STATE).
func newHA3(t *testing.T, ctx context.Context, b *bed) *ha3 {
	k := newKit(t, ctx)
	home := t.TempDir()
	if b.State != "" {
		home = filepath.Join(b.State, "home")
		_ = os.MkdirAll(home, 0o700)
	}
	cfg := filepath.Join(home, ".kevaso", "config.yaml")
	c := &cluster{K: k, Home: home, Cfg: cfg, Env: append(os.Environ(), "HOME="+home, "KVO_CONFIG="+cfg, "KVO_SERVER=", "KVO_TOKEN=")}
	h := &ha3{t: t, ctx: ctx, b: b, k: k, c: c, recv: filepath.Join(home, "recovery-key"), built: map[string]string{}}
	h.diagnoseOnFailure()
	if ms, ok := b.Restore(t); ok {
		h.ms, h.ssh = ms, b.SSHFlags()
		t.Logf("reusing the cluster kept in %s", b.State)
		return h
	}
	h.ms = b.CreateAll(ctx, "cp1", "cp2", "cp3", "w1", "w2")
	b.Save(t, h.ms)
	h.ssh = b.SSHFlags()
	kevasod := h.kevasod(haVersion)
	start := time.Now()
	m := h.ms["cp1"]
	out := h.mustKvoadm("", append([]string{"init", "root@" + m.Public, "--external-ip", m.Public, "--kevasod", kevasod, "--name", "cp1", "--cluster", "ha3",
		"--store", "etcd", "--api-server", "https://" + m.Public + ":6443"}, h.ssh...)...)
	key := regexp.MustCompile(`KVR1-[A-Z0-9-]+`).FindString(out)
	if key == "" || !strings.Contains(out, "cluster ha3 created on this host") {
		t.Fatalf("kvoadm init: no recovery key, or no cluster record:\n%s", redact(out))
	}
	if err := os.WriteFile(h.recv, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("kvoadm init:\n%s", redact(out))
	for _, w := range []string{"w1", "w2"} {
		m := h.ms[w]
		t.Log(h.mustKvoadm("", append([]string{"host", "add", "root@" + m.Public, "--external-ip", m.Public, "--kevasod", kevasod, "--name", w,
			"--install", "worker", "--runtime", "docker", "--region", b.Region, "--label", "kvt/host=" + w}, h.ssh...)...))
	}
	for _, n := range []string{"cp2", "cp3"} {
		m := h.ms[n]
		t.Log(h.mustKvoadm("", append([]string{"host", "add", "root@" + m.Public, "--external-ip", m.Public, "--kevasod", kevasod, "--name", n,
			"--install", "control-plane"}, h.ssh...)...))
	}
	t.Logf("cluster made in %s", time.Since(start).Round(time.Second))
	return h
}

var recoveryKey = regexp.MustCompile(`KVR1-[A-Z0-9-]+`)

func redact(s string) string { return recoveryKey.ReplaceAllString(s, "KVR1-(redacted)") }

func (h *ha3) diagnoseOnFailure() {
	h.t.Cleanup(func() { // before the sweep
		if !h.t.Failed() || h.ms == nil {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		for _, n := range []string{"cp1", "cp2", "cp3", "w1", "w2"} {
			out, _ := h.b.SSH(dctx, h.ms[n], "systemctl is-active kevasod kevaso-fence 2>&1 | tr '\\n' ' '; echo; nft list tables 2>&1 | tr '\\n' ' '; echo; "+
				"journalctl -u kevasod -n 100 --no-pager 2>&1 | grep -iv 'heartbeat' | tail -60; docker ps -a --format '{{.Names}} {{.Status}}' 2>/dev/null | head -20")
			h.t.Logf("── %s ──\n%s", n, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "wide"}, {"get", "silos", "-A", "-o", "wide"}} {
			out, _ := h.c.Kvo("", args...)
			h.t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})
}

// kevasod is a Linux build of core's kevasod stamped version (built once per version).
func (h *ha3) kevasod(version string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p, ok := h.built[version]; ok {
		return p
	}
	p := h.k.Build("kevasod-"+version, "kevaso.com/core/cmd/kevasod", "linux", version)
	h.built[version] = p
	return p
}

func (h *ha3) kvoadm(stdin string, args ...string) (string, error) {
	if stdin == "" {
		return h.c.Kvoadm(args...)
	}
	return h.c.Run(stdin, h.k.Kvoadm, args...)
}

func (h *ha3) mustKvoadm(stdin string, args ...string) string {
	h.t.Helper()
	out, err := h.kvoadm(stdin, args...)
	if err != nil {
		h.t.Fatalf("kvoadm %s: %v\n%s", strings.Join(args[:min(3, len(args))], " "), err, redact(out))
	}
	return out
}

// reset runs kvoadm reset on a machine. Core 91b4ead's reset stops kevasod with one
// `systemctl disable --now kevasod.service kevaso-fence.service`, which fails as a whole on a
// host with no fence unit (a system node), so it refuses: "kevasod or its fence still runs after
// it was stopped". That refusal is logged as a finding and, unless KEVASO_E2E_STRICT is set, the
// step it names is taken (stop kevasod by hand) and the reset run again.
func (h *ha3) reset(t *testing.T, m string) string {
	t.Helper()
	args := append([]string{"reset", "root@" + h.ms[m].Public, "--yes", "--force"}, h.ssh...)
	out, err := h.kvoadm("", args...)
	if err != nil && strings.Contains(out, "still runs after it was stopped") {
		t.Logf("FINDING (core, host lane): kvoadm reset %s refused: kevasod not stopped by its stop step: %s", m, lastLine(out))
		if os.Getenv("KEVASO_E2E_STRICT") != "" {
			t.Fatalf("kvoadm reset %s: %v", m, err)
		}
		h.mustSh(t, m, "systemctl disable --now kevasod")
		out, err = h.kvoadm("", args...)
	}
	if err != nil {
		t.Fatalf("kvoadm reset %s: %v\n%s", m, err, out)
	}
	return out
}

func lastLine(s string) string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	return l[len(l)-1]
}

// sh runs a command as root on a machine.
func (h *ha3) sh(m, cmd string) (string, error) { return h.b.SSH(h.ctx, h.ms[m], cmd) }

func (h *ha3) mustSh(t *testing.T, m, cmd string) string {
	t.Helper()
	out, err := h.sh(m, cmd)
	if err != nil {
		t.Fatalf("%s: %s: %v\n%s", m, cmd, err, out)
	}
	return out
}

// kvoWithin runs kvo, through the context's servers (or at url when not ""), killed after d: a
// poll that never answers is a failed poll, never a stuck test.
func (h *ha3) kvoWithin(d time.Duration, url string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(h.ctx, d)
	defer cancel()
	cfg := h.c.Cfg
	if url != "" {
		b, err := os.ReadFile(h.c.Cfg)
		if err != nil {
			return "", err
		}
		cfg = filepath.Join(filepath.Dir(h.c.Cfg), "within-"+regexp.MustCompile(`[^0-9a-z]+`).ReplaceAllString(url, "_")+".yaml")
		if err := os.WriteFile(cfg, []byte(regexp.MustCompile(`(?m)^(\s*server:\s*).*$`).ReplaceAllString(string(b), "${1}"+url)), 0o600); err != nil {
			return "", err
		}
	}
	cmd := exec.CommandContext(ctx, h.k.Kvo, args...)
	cmd.Env = append(append([]string(nil), h.c.Env...), "KVO_CONFIG="+cfg)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("no answer in %s", d)
	}
	return string(out), err
}

// hostsWithin is Hosts with a deadline.
func (h *ha3) hostsWithin(d time.Duration) (map[string]hostView, error) {
	out, err := h.kvoWithin(d, "", "get", "hosts", "-o", "json")
	if err != nil {
		return nil, err
	}
	l, err := decodeList[hostView](out)
	if err != nil {
		return nil, err
	}
	m := map[string]hostView{}
	for _, x := range l {
		m[x.Metadata.Name] = x
	}
	return m, nil
}

// ── the cluster's state ──────────────────────────────────────────────────────────────────────

func (h *ha3) hosts(t *testing.T) map[string]hostView {
	t.Helper()
	hs, err := h.hostsWithin(30 * time.Second)
	if err != nil {
		t.Fatalf("kvo get hosts: %v", err)
	}
	return hs
}

func condition(hv hostView, typ string) (string, string, string) {
	for _, c := range hv.Status.Conditions {
		if c.Type == typ {
			return c.Status, c.Reason, c.Message
		}
	}
	return "", "", ""
}

// running says which machines are on (as Linode sees it).
func (h *ha3) poweredOn(m string) bool {
	in, err := h.b.API.GetInstance(h.ctx, h.ms[m].ID)
	return err == nil && in.Status == "running"
}

// healthy waits until the cluster is as every part expects to find it, putting back what a
// part that failed may have left: every machine on, nothing cut or silenced, all five Ready, three
// voters and one leader.
func (h *ha3) healthy(t *testing.T) {
	t.Helper()
	for _, n := range []string{"cp1", "cp2", "cp3", "w1", "w2"} {
		if !h.poweredOn(n) {
			t.Logf("%s is off: powering it on", n)
			h.b.PowerOn(h.ctx, h.ms[n])
		}
		_, _ = h.sh(n, "nft delete table inet kvt-cut 2>/dev/null; nft delete table inet kvt-silent 2>/dev/null; true")
	}
	eventually(t, "all five Ready, three voters, one leader", 10*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		var s []string
		ok := true
		for _, n := range []string{"cp1", "cp2", "cp3", "w1", "w2"} {
			s = append(s, fmt.Sprintf("%s ready=%v member=%s", n, hs[n].Ready(), hs[n].Member()))
			ok = ok && hs[n].Ready() && (!slices.Contains(cps, n) || hs[n].Member() == "voter")
		}
		l := leaders(hs, cps...)
		return ok && len(l) == 1, strings.Join(s, ", ") + fmt.Sprintf(" leaders=%v", l)
	})
}

// clearLeftovers deletes what a stopped run's parts left in their namespaces (only at a part's
// start: a part calls healthy in its middle).
func (h *ha3) clearLeftovers(t *testing.T) {
	for _, ns := range []string{"ha-a", "ha-b", "ha-c", "ha-d", "ha-e", "ha-f"} {
		if out, err := h.kvoWithin(time.Minute, "", "get", "silos", "-n", ns, "-o", "json"); err == nil {
			if l, _ := decodeList[haSilo](out); len(l) > 0 {
				t.Logf("clearing %d silos left in %s", len(l), ns)
				h.clearNamespace(t, ns)
			}
		}
	}
}

func (h *ha3) leader(t *testing.T) string {
	t.Helper()
	var lead string
	eventually(t, "one leader", 3*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, cps...)
		if len(l) == 1 {
			lead = l[0]
		}
		return len(l) == 1, fmt.Sprint(l)
	})
	return lead
}

func others(of string) []string {
	var out []string
	for _, n := range cps {
		if n != of {
			out = append(out, n)
		}
	}
	return out
}

func (h *ha3) url(n string) string { return "https://" + h.ms[n].Public + ":6443" }

// ── silos ────────────────────────────────────────────────────────────────────────────────────

type haSilo struct {
	Metadata struct {
		Name            string `json:"name"`
		UID             string `json:"uid"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Status struct {
		Phase      string `json:"phase"`
		Host       string `json:"host"`
		MovingFrom string `json:"movingFrom"`
		Conditions []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func (s haSilo) cond(typ string) (string, string) {
	for _, c := range s.Status.Conditions {
		if c.Type == typ {
			return c.Status, c.Reason
		}
	}
	return "", ""
}

// siloYAML is a docker silo running nginx, pinned to a worker when on isn't "", with extra spec
// lines (availability, volumes) indented under spec.
func siloYAML(ns, name, on string, extra ...string) string {
	s := `apiVersion: kevaso/v1
kind: Silo
metadata: { name: ` + name + `, namespace: ` + ns + ` }
spec:
  runtime: docker
  container: { image: docker.io/library/nginx:1.27-alpine }
  resources: { memory: 64Mi }
`
	if on != "" {
		s += "  placement: { hostSelector: { kvt/host: " + on + " } }\n"
	}
	for _, e := range extra {
		s += "  " + e + "\n"
	}
	return s
}

func (h *ha3) namespace(t *testing.T, ns string) {
	t.Helper()
	h.c.MustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: "+ns+" }\n", "apply", "-f", "-")
	t.Cleanup(func() { h.clearNamespace(t, ns) })
}

// clearNamespace leaves the cluster as a part found it: each of the namespace's silos deleted
// (a Namespace's delete leaves its silos running: run 2 of TestHA3, reported to core), then the
// namespace.
func (h *ha3) clearNamespace(t *testing.T, ns string) {
	out, _ := h.kvoWithin(time.Minute, "", "get", "silos", "-n", ns, "-o", "json")
	l, _ := decodeList[haSilo](out)
	for _, s := range l {
		if o, err := h.kvoWithin(time.Minute, "", "delete", "silo", s.Metadata.Name, "-n", ns); err != nil {
			t.Logf("delete silo %s/%s: %v %s", ns, s.Metadata.Name, err, firstLine(o))
		}
	}
	for deadline := time.Now().Add(5 * time.Minute); ; time.Sleep(5 * time.Second) {
		out, _ := h.kvoWithin(time.Minute, "", "get", "silos", "-n", ns, "-o", "json")
		if l, _ := decodeList[haSilo](out); len(l) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("namespace %s: silos still listed after 5 minutes", ns)
			return
		}
	}
	if out, err := h.kvoWithin(time.Minute, "", "delete", "namespace", ns); err != nil {
		t.Logf("delete namespace %s: %v %s", ns, err, firstLine(out))
	}
}

func (h *ha3) apply(t *testing.T, yaml string) {
	t.Helper()
	h.c.MustKvo(yaml, "apply", "-f", "-")
}

func (h *ha3) silo(ns, name string) (haSilo, error) {
	var s haSilo
	out, err := h.c.Kvo("", "get", "silo", name, "-n", ns, "-o", "json")
	if err != nil {
		return s, fmt.Errorf("%v: %s", err, firstLine(out))
	}
	return s, json.Unmarshal([]byte(out), &s)
}

func (h *ha3) silos(t *testing.T, ns string) []haSilo {
	t.Helper()
	out := h.c.MustKvo("", "get", "silos", "-n", ns, "-o", "json")
	l, err := decodeList[haSilo](out)
	if err != nil {
		t.Fatalf("kvo get silos -n %s: %v", ns, err)
	}
	return l
}

func (h *ha3) waitRunning(t *testing.T, ns string, d time.Duration, names ...string) map[string]haSilo {
	t.Helper()
	got := map[string]haSilo{}
	eventually(t, ns+": "+strings.Join(names, ", ")+" running", d, func() (bool, string) {
		var not []string
		for _, n := range names {
			s, err := h.silo(ns, n)
			if err != nil || s.Status.Phase != "Running" || s.Status.Host == "" || s.Status.MovingFrom != "" {
				not = append(not, fmt.Sprintf("%s=%s@%s %v", n, s.Status.Phase, s.Status.Host, err))
				continue
			}
			got[n] = s
		}
		return len(not) == 0, strings.Join(not, " ")
	})
	return got
}

// containers are a silo's own running containers on a machine (not its pause container), by
// UID: "ID STARTED RUNNING" each.
func (h *ha3) containers(m, uid string) ([]string, error) {
	out, err := h.sh(m, "for c in $(docker ps -q --filter label=kevaso.com/uid="+uid+" --filter label=kevaso.com/name); do "+
		"docker inspect -f '{{.Id}} {{.State.StartedAt}} {{.State.Running}}' $c; done")
	if err != nil {
		return nil, fmt.Errorf("%v %s", err, out)
	}
	var cs []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l != "" {
			cs = append(cs, l)
		}
	}
	return cs, nil
}

// runs says whether the silo has a running container on m.
func (h *ha3) runs(m, uid string) (bool, error) {
	cs, err := h.containers(m, uid)
	for _, c := range cs {
		if strings.HasSuffix(c, " true") {
			return true, err
		}
	}
	return false, err
}

// one is the silo's single running container on m.
func (h *ha3) one(t *testing.T, m, uid string) string {
	t.Helper()
	cs, err := h.containers(m, uid)
	if err != nil || len(cs) != 1 || !strings.HasSuffix(cs[0], " true") {
		t.Fatalf("silo %s on %s: want one running container, have %q (%v)", uid, m, cs, err)
	}
	return cs[0]
}

// ── network cuts, on the machine itself (an nftables table of the test's, never port 22) ─────

// addrsOf are a machine's public and private IPv4s.
func (h *ha3) addrsOf(names ...string) []string {
	var out []string
	for _, n := range names {
		out = append(out, h.ms[n].Public)
		if h.ms[n].Private != "" {
			out = append(out, h.ms[n].Private)
		}
	}
	return out
}

// cut drops all traffic between machine m and the machines from, on m.
func (h *ha3) cut(t *testing.T, m string, from ...string) {
	t.Helper()
	set := "{ " + strings.Join(h.addrsOf(from...), ", ") + " }"
	h.mustSh(t, m, `nft -f - <<'NFT'
table inet kvt-cut {
  chain in { type filter hook input priority -10; ip saddr `+set+` drop; }
  chain out { type filter hook output priority -10; ip daddr `+set+` drop; }
}
NFT`)
}

// cutRejecting is cut, but every TCP packet is answered with a reset (and the rest refused): the
// worker's connections fail at once, as with a machine that is up but refuses, never hang. That
// keeps K-008 (a hang without failover) out of what a test asks.
func (h *ha3) cutRejecting(t *testing.T, m string, from ...string) {
	t.Helper()
	set := "{ " + strings.Join(h.addrsOf(from...), ", ") + " }"
	h.mustSh(t, m, `nft -f - <<'NFT'
table inet kvt-cut {
  chain in { type filter hook input priority -10; ip saddr `+set+` meta l4proto tcp reject with tcp reset; ip saddr `+set+` drop; }
  chain out { type filter hook output priority -10; ip daddr `+set+` meta l4proto tcp reject with tcp reset; ip daddr `+set+` reject; }
}
NFT`)
}

func (h *ha3) uncut(m string) { _, _ = h.sh(m, "nft delete table inet kvt-cut 2>/dev/null; true") }

// silence makes m answer nothing at all but SSH: powered on, its processes running, every other
// packet dropped (a system node that goes silent, K-008's case).
func (h *ha3) silence(t *testing.T, m string) {
	t.Helper()
	h.mustSh(t, m, `nft -f - <<'NFT'
table inet kvt-silent {
  chain in { type filter hook input priority -20; iif lo accept; tcp dport 22 accept; drop; }
  chain out { type filter hook output priority -20; oif lo accept; tcp sport 22 accept; drop; }
}
NFT`)
}

func (h *ha3) unsilence(m string) {
	_, _ = h.sh(m, "nft delete table inet kvt-silent 2>/dev/null; true")
}

// ── a: the leader lost, then the quorum, then a silent leader ───────────────────────────────

// watch polls, until stop, what the workers do: whether each answers its heartbeat (Ready, a
// heartbeat younger than lease), and whether each given silo still runs its container.
type watch struct {
	mu         sync.Mutex
	polls      int
	stale      map[string]int    // worker → polls where its heartbeat was older than the lease
	staleFirst map[string]string // worker → when first stale
	stopped    map[string]string // silo name → when its container was first seen not running
	apiFail    int
	apiMaxGap  time.Duration
	apiLastOK  time.Time
	stop       chan struct{}
	done       sync.WaitGroup
}

func (h *ha3) watchWorkers(silos map[string]haSilo) *watch {
	w := &watch{stale: map[string]int{}, staleFirst: map[string]string{}, stopped: map[string]string{}, stop: make(chan struct{}), apiLastOK: time.Now()}
	w.done.Add(1)
	go func() {
		defer w.done.Done()
		for {
			select {
			case <-w.stop:
				return
			case <-time.After(5 * time.Second):
			}
			hs, err := h.hostsWithin(20 * time.Second)
			now := time.Now()
			w.mu.Lock()
			w.polls++
			if err != nil {
				w.apiFail++
			} else {
				if gap := now.Sub(w.apiLastOK); gap > w.apiMaxGap {
					w.apiMaxGap = gap
				}
				w.apiLastOK = now
				for _, n := range []string{"w1", "w2"} {
					hb := hs[n].Status.LastHeartbeat
					if hb == nil || now.Sub(*hb) > coreHostLease || !hs[n].Ready() {
						w.stale[n]++
						if w.staleFirst[n] == "" {
							w.staleFirst[n] = now.Format(time.TimeOnly)
						}
					}
				}
			}
			w.mu.Unlock()
			for name, s := range silos {
				if ok, err := h.runs(s.Status.Host, s.Metadata.UID); err == nil && !ok {
					w.mu.Lock()
					if w.stopped[name] == "" {
						w.stopped[name] = time.Now().Format(time.TimeOnly)
					}
					w.mu.Unlock()
				}
			}
		}
	}()
	return w
}

func (w *watch) end() *watch { close(w.stop); w.done.Wait(); return w }

func (w *watch) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("%d polls: API failed %d, longest without an answer %s; workers' heartbeat older than the lease: %v (first %v); silos seen stopped: %v",
		w.polls, w.apiFail, w.apiMaxGap.Round(time.Second), w.stale, w.staleFirst, w.stopped)
}

// ha3Leader (a): the leader powered off; then two of three off (no quorum); then a leader that
// goes silent while powered on (K-008). Workers keep everything throughout, the API answers
// again in a stated time, nothing is lost.
func ha3Leader(t *testing.T, h *ha3) {
	const ns = "ha-a"
	const after = 60 * time.Second
	h.namespace(t, ns)
	stop := "availability: { onControlPlaneLoss: Stop, after: 60s }"
	h.apply(t, siloYAML(ns, "plain-w1", "w1")+"---\n"+siloYAML(ns, "plain-w2", "w2")+"---\n"+
		siloYAML(ns, "stop-w1", "w1", stop)+"---\n"+siloYAML(ns, "stop-w2", "w2", stop))
	names := []string{"plain-w1", "plain-w2", "stop-w1", "stop-w2"}
	ss := h.waitRunning(t, ns, 10*time.Minute, names...)
	before := map[string]string{}
	for n, s := range ss {
		before[n] = h.one(t, s.Status.Host, s.Metadata.UID)
	}
	same := func(when string, which ...string) {
		t.Helper()
		for _, n := range which {
			s := ss[n]
			if cs, err := h.containers(s.Status.Host, s.Metadata.UID); err != nil || len(cs) != 1 || cs[0] != before[n] {
				t.Errorf("%s: %s's container changed or stopped on %s: %q, was %q (%v)", when, n, s.Status.Host, cs, before[n], err)
			}
		}
	}

	// 1. The leader powered off.
	lead := h.leader(t)
	rest := others(lead)
	w := h.watchWorkers(ss)
	offAt := time.Now()
	h.b.PowerOff(h.ctx, h.ms[lead])
	t.Logf("a1: %s (the leader) powered off: off %s after the shutdown call", lead, time.Since(offAt).Round(time.Second))
	var newLead string
	eventually(t, "a new leader among "+strings.Join(rest, ", "), 3*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, rest...)
		if len(l) > 1 {
			t.Fatalf("two leaders at once: %v", l)
		}
		if len(l) == 1 {
			newLead = l[0]
		}
		return len(l) == 1, fmt.Sprint(l)
	})
	leadIn := time.Since(offAt)
	h.apply(t, siloYAML(ns, "after-lead", ""))
	h.waitRunning(t, ns, 5*time.Minute, "after-lead")
	writeIn := time.Since(offAt)
	t.Logf("a1: %s leads %s after the power-off call; a new silo was scheduled and runs %s after it", newLead, leadIn.Round(time.Second), writeIn.Round(time.Second))
	if writeIn > 3*time.Minute {
		t.Errorf("a1: the API took %s to take a write and schedule it (stated: within 3 minutes of the power-off call, a shutdown included)", writeIn.Round(time.Second))
	}
	time.Sleep(after + coreHostLease) // past the Stop silos' after, and past the host lease
	w.end()
	t.Logf("a1: while %s was off: %s", lead, w)
	same("a1 (one system node off)", names...)
	if len(w.stale) > 0 || len(w.stopped) > 0 {
		t.Errorf("a1: K-008 SEEN: with %s powered off and two system nodes serving, a worker's heartbeat went stale or an opt-in silo stopped: %s", lead, w)
	} else {
		t.Logf("a1: K-008 not seen: both workers kept their heartbeat through the others (never older than %s), the Stop silos never stopped", coreHostLease)
	}
	if w.apiMaxGap > 30*time.Second {
		t.Errorf("a1: kvo had no answer for %s (stated: 30s at most, one failed connect being 5s)", w.apiMaxGap.Round(time.Second))
	}

	// 2. Two of three off: no quorum. Workers keep running everything that didn't opt in, and
	// nothing changes; the opt-in silos stop after their after (that is what they asked for), and
	// are started again on their own hosts once the control plane is back. Nothing is lost.
	second := rest[0]
	if rest[0] == newLead {
		second = rest[1]
	}
	survivor := newLead
	objects := func() []string {
		var out []string
		for _, s := range h.silos(t, ns) {
			out = append(out, s.Metadata.Name+" "+s.Metadata.UID+" "+s.Status.Host)
		}
		sort.Strings(out)
		return out
	}
	objBefore := objects()
	w = h.watchWorkers(ss)
	h.b.PowerOff(h.ctx, h.ms[second])
	noQuorum := time.Now()
	t.Logf("a2: %s off too: %s alone, reachable, without a quorum", second, survivor)
	reads, readsOK, writesOK := 0, 0, 0
	var readErr, writeErr string
	for end := time.Now().Add(after + coreFenceSlack + 60*time.Second); time.Now().Before(end); time.Sleep(10 * time.Second) {
		reads++
		if out, err := h.kvoWithin(30*time.Second, h.url(survivor), "get", "silos", "-n", ns); err == nil && strings.Contains(out, "plain-w1") {
			readsOK++
		} else {
			readErr = fmt.Sprint(err, " ", firstLine(out))
		}
		if out, err := h.kvoWithin(30*time.Second, h.url(survivor), "create", "secret", fmt.Sprintf("nq%d", reads), "-n", ns, "--from-file", "/dev/null"); err == nil {
			writesOK++
		} else {
			writeErr = fmt.Sprint(err, " ", firstLine(out))
		}
	}
	t.Logf("a2: at %s without a quorum: reads answered %d of %d (last failure: %s); writes taken %d of %d (last failure: %s)",
		survivor, readsOK, reads, readErr, writesOK, reads, writeErr)
	if readsOK < reads && strings.Contains(readErr, "no answer in") {
		t.Logf("a2: FINDING: a read at a reachable system node without a quorum got no answer in 30s (no error either)")
	}
	w.end()
	t.Logf("a2: no quorum for %s: %s; kvo get silos at %s answered %d of %d", time.Since(noQuorum).Round(time.Second), w, survivor, readsOK, reads)
	same("a2 (no quorum)", "plain-w1", "plain-w2")
	for _, n := range []string{"stop-w1", "stop-w2"} {
		if w.stopped[n] == "" {
			t.Logf("a2: %s did not stop without a quorum (its worker counted the quorum-less %s as contact)", n, survivor)
		}
	}
	h.b.PowerOn(h.ctx, h.ms[second])
	h.b.PowerOn(h.ctx, h.ms[lead])
	h.healthy(t)
	t.Logf("a2: quorum back %s after it was lost", time.Since(noQuorum).Round(time.Second))
	same("a2 (after)", "plain-w1", "plain-w2")
	h.waitRunning(t, ns, 5*time.Minute, names...)
	for _, n := range []string{"stop-w1", "stop-w2"} {
		s, _ := h.silo(ns, n)
		if s.Status.Host != ss[n].Status.Host {
			t.Errorf("a2: %s moved to %q (it asked to stop, not to be restarted elsewhere)", n, s.Status.Host)
		}
		other := map[string]string{"w1": "w2", "w2": "w1"}[s.Status.Host]
		if r, _ := h.runs(other, s.Metadata.UID); r {
			t.Errorf("a2: %s runs on %s too", n, other)
		}
		before[n] = h.one(t, s.Status.Host, s.Metadata.UID)
	}
	if got := objects(); strings.Join(got, "\n") != strings.Join(objBefore, "\n") {
		t.Errorf("a2: objects changed across the outage:\nbefore %v\nafter  %v", objBefore, got)
	}
	t.Log("a2: every silo object kept with its UID and host; plain silos never restarted")

	// 3. A system node goes silent, powered on (K-008's own case): the one the workers hold their
	// connections to (the leader if they hold none). The others serve, the workers must fail
	// over to them, and the Stop silos must not stop.
	lead = h.talkedTo(t)
	w = h.watchWorkers(ss)
	t.Cleanup(func() { h.unsilence(lead) })
	h.silence(t, lead)
	silentAt := time.Now()
	t.Logf("a3: %s silent: everything but SSH dropped", lead)
	eventually(t, "a leader among the others", 3*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, others(lead)...)
		return len(l) == 1, fmt.Sprint(l)
	})
	t.Logf("a3: another leads %s after the silence", time.Since(silentAt).Round(time.Second))
	time.Sleep(after + coreHostLease)
	w.end()
	h.unsilence(lead)
	t.Logf("a3: while %s was silent: %s", lead, w)
	same("a3 (a silent system node)", names...)
	if len(w.stale) > 0 || len(w.stopped) > 0 {
		t.Errorf("a3: K-008 SEEN: with %s silent and two system nodes serving, a worker's heartbeat went stale or an opt-in silo stopped: %s", lead, w)
	} else {
		t.Log("a3: K-008 not seen: the workers failed over from the silent node; nothing stopped")
	}
	h.healthy(t)
}

// talkedTo is the system node most of the workers' connections to an API port go to (from the
// workers' own socket tables), or the leader when they hold none.
func (h *ha3) talkedTo(t *testing.T) string {
	t.Helper()
	count := map[string]int{}
	for _, w := range []string{"w1", "w2"} {
		out, _ := h.sh(w, "ss -Htn state established '( dport = :6443 )' | awk '{print $4}'")
		for _, a := range strings.Fields(out) {
			ip, _, _ := strings.Cut(a, ":")
			for _, n := range cps {
				if slices.Contains(h.addrsOf(n), ip) {
					count[n]++
				}
			}
		}
	}
	best := ""
	for _, n := range cps {
		if count[n] > count[best] {
			best = n
		}
	}
	t.Logf("the workers' connections to system nodes: %v", count)
	if best == "" {
		return h.leader(t)
	}
	return best
}

// ── b: a worker cut off from every system node ──────────────────────────────────────────────

// ha3Cut (b): w1 cut off from the three system nodes. A silo without the opt-in keeps running
// there and is not started elsewhere; a Stop + Recreate silo stops after its after, is started on
// w2, and at no moment do both run it. The cut is lifted the moment the restart is committed.
func ha3Cut(t *testing.T, h *ha3) {
	const ns = "ha-b"
	const after = 30 * time.Second
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "keep", "w1")+"---\n"+
		siloYAML(ns, "once", "", "availability: { onControlPlaneLoss: Stop, after: 30s, onHostLoss: Recreate }"))
	ss := h.waitRunning(t, ns, 10*time.Minute, "keep", "once")
	if ss["once"].Status.Host != "w1" {
		h.c.MustKvo("", "move", "silo", "once", "-n", ns, "--to", "w1")
		eventually(t, "once on w1", 5*time.Minute, func() (bool, string) {
			s, _ := h.silo(ns, "once")
			return s.Status.Host == "w1" && s.Status.Phase == "Running" && s.Status.MovingFrom == "", s.Status.Host + " " + s.Status.Phase
		})
		ss = h.waitRunning(t, ns, time.Minute, "keep", "once")
	}
	once, keep := ss["once"].Metadata.UID, ss["keep"]
	eventually(t, "once's fence armed on w1", 2*time.Minute, func() (bool, string) {
		s, _ := h.silo(ns, "once")
		st, r := s.cond("FenceArmed")
		return st == "True", st + " " + r
	})
	keepBefore := h.one(t, "w1", keep.Metadata.UID)

	// Both workers asked about once, side by side, every half second, from here.
	var both atomic.Value
	var checks atomic.Int64
	var stoppedW1, startedW2 atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r1, r2 := make(chan bool, 1), make(chan bool, 1)
			go func() { ok, _ := h.runs("w1", once); r1 <- ok }()
			go func() { ok, _ := h.runs("w2", once); r2 <- ok }()
			a, b := <-r1, <-r2
			now := time.Now()
			if a && b && both.Load() == nil {
				both.Store(now)
			}
			if !a && stoppedW1.Load() == nil {
				stoppedW1.Store(now)
			}
			if b && startedW2.Load() == nil {
				startedW2.Store(now)
			}
			checks.Add(1)
			time.Sleep(500 * time.Millisecond)
		}
	}()
	t.Cleanup(func() { h.uncut("w1") })
	h.cut(t, "w1", cps...)
	cutAt := time.Now()
	t.Log("b: w1 cut off from cp1, cp2, cp3 (public and private addresses; SSH from here untouched)")
	var committed time.Time
	for deadline := time.Now().Add(6 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		s, err := h.silo(ns, "once")
		if err == nil && s.Status.Host != "" && s.Status.Host != "w1" {
			h.uncut("w1")
			committed = time.Now()
			t.Logf("b: the restart on %q committed %s into the cut: the cut lifted at once", s.Status.Host, committed.Sub(cutAt).Round(time.Second))
			break
		}
		if time.Now().After(deadline) {
			h.uncut("w1")
			t.Fatal("b: the control plane never restarted once elsewhere")
		}
	}
	h.waitRunning(t, ns, 5*time.Minute, "once")
	eventually(t, "w1 Ready again", 5*time.Minute, func() (bool, string) {
		hv := h.hosts(t)["w1"]
		return hv.Ready(), fmt.Sprint(hv.Status.Conditions)
	})
	time.Sleep(3 * time.Minute) // both machines asked meanwhile, and after
	close(stop)
	wg.Wait()
	if at, ok := both.Load().(time.Time); ok {
		t.Errorf("b: both workers ran once at %s (%s into the cut): two copies", at.Format(time.TimeOnly), at.Sub(cutAt).Round(time.Second))
	} else {
		t.Logf("b: never two copies: both workers asked %d times, every half second", checks.Load())
	}
	if at, ok := stoppedW1.Load().(time.Time); !ok {
		t.Error("b: once never stopped on w1")
	} else if d := at.Sub(cutAt); d < after-coreHeartbeat {
		t.Errorf("b: once stopped on w1 %s into the cut, before its after (%s, less a heartbeat)", d.Round(time.Second), after)
	} else {
		t.Logf("b: once stopped on w1 %s into the cut (after %s)", d.Round(time.Second), after)
	}
	if d := committed.Sub(cutAt); d < after+stopMargin(after)-coreHeartbeat {
		t.Errorf("b: restarted elsewhere %s into the cut, before after + margin (%s)", d.Round(time.Second), after+stopMargin(after))
	}
	if at, ok := startedW2.Load().(time.Time); ok {
		t.Logf("b: once running on w2 %s into the cut", at.Sub(cutAt).Round(time.Second))
	}
	if r, _ := h.runs("w1", once); r {
		t.Error("b: w1 still runs a copy of once after the cut")
	}
	if got := h.one(t, "w1", keep.Metadata.UID); got != keepBefore {
		t.Errorf("b: keep restarted on w1: %s, was %s", got, keepBefore)
	}
	if s, _ := h.silo(ns, "keep"); s.Status.Host != "w1" {
		t.Errorf("b: keep (no opt-in) placed on %q: the control plane restarted it elsewhere", s.Status.Host)
	}
	t.Log("b: keep (no opt-in) ran on w1 throughout and was never placed elsewhere")
}

// ── c: a system node back after a leader change and writes; a worker that talks to it first ──

// ha3Rejoin (c): the leader powered off (so the leadership changes while it's away), several
// writes, then it comes back while w1 can reach only it: it catches up, and w1, talking to it
// first, does not hold (the "older control plane" rule must not trip on a healthy node that is
// only behind for a moment: the auditor's K-071 concern).
func ha3Rejoin(t *testing.T, h *ha3) {
	const ns = "ha-c"
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "base", "w1"))
	ss := h.waitRunning(t, ns, 10*time.Minute, "base")
	baseBefore := h.one(t, "w1", ss["base"].Metadata.UID)
	x := h.leader(t)
	rest := others(x)
	h.b.PowerOff(h.ctx, h.ms[x])
	t.Logf("c: %s (the leader) powered off", x)
	eventually(t, "a new leader", 3*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, rest...)
		return len(l) == 1, fmt.Sprint(l)
	})
	// Writes while it's away: new silos, a changed one, a deleted one, a secret.
	h.apply(t, siloYAML(ns, "while-1", "w1")+"---\n"+siloYAML(ns, "while-2", "w2")+"---\n"+siloYAML(ns, "gone", "w2"))
	h.waitRunning(t, ns, 5*time.Minute, "while-1", "while-2", "gone")
	h.c.MustKvo("", "delete", "silo", "gone", "-n", ns)
	h.apply(t, strings.Replace(siloYAML(ns, "while-2", "w2"), "namespace: "+ns+" }", "namespace: "+ns+", labels: { kvt/changed: \"yes\" } }", 1))
	h.c.MustKvo("written while "+x+" was off", "create", "secret", "s1", "-n", ns, "--from-file", "-")
	for i := 0; i < 20; i++ { // and many revisions
		h.c.MustKvo(fmt.Sprintf("value %d", i), "create", "secret", fmt.Sprintf("n%d", i), "-n", ns, "--from-file", "-")
	}
	h.waitRunning(t, ns, 5*time.Minute, "while-1", "while-2")
	latest, _ := h.silo(ns, "while-2")
	t.Logf("c: writes made while %s was off (while-2 at revision %s)", x, latest.Metadata.ResourceVersion)

	// w1 can reach only x from now on: its first answers after x is back are x's.
	t.Cleanup(func() { h.uncut("w1") })
	// Refused, not dropped: dropped packets look like a silent node, and K-008 (no failover
	// from a hung request) would keep w1 off x before the hold rule is ever asked (run 2).
	h.cutRejecting(t, "w1", rest...)
	cutAt := time.Now()
	since := strings.TrimSpace(h.mustSh(t, "w1", "date '+%Y-%m-%d %H:%M:%S'"))
	h.b.PowerOn(h.ctx, h.ms[x])
	t.Logf("c: %s back on; w1 reaches only it", x)
	eventually(t, x+" serving what was written while it was off", 5*time.Minute, func() (bool, string) {
		out, err := h.c.KvoAt(h.url(x), "get", "silo", "while-2", "-n", ns, "-o", "json")
		var s haSilo
		_ = json.Unmarshal([]byte(out), &s)
		return err == nil && s.Metadata.ResourceVersion == latest.Metadata.ResourceVersion, fmt.Sprintf("%v rv=%s want %s", err, s.Metadata.ResourceVersion, latest.Metadata.ResourceVersion)
	})
	if out, err := h.c.KvoAt(h.url(x), "get", "secret", "n19", "-n", ns); err != nil {
		t.Errorf("c: %s doesn't have the last secret written while it was off: %v %s", x, err, out)
	}
	if out, err := h.c.KvoAt(h.url(x), "get", "silo", "gone", "-n", ns); err == nil {
		t.Errorf("c: %s still lists the silo deleted while it was off: %s", x, out)
	}
	t.Logf("c: %s caught up %s after power-on: same revision, the secret, the delete", x, time.Since(cutAt).Round(time.Second))
	var behind string
	eventually(t, "w1 Ready again, through "+x+" alone", 5*time.Minute, func() (bool, string) {
		hv := h.hosts(t)["w1"]
		st, r, msg := condition(hv, "ControlPlaneBehind")
		if st == "True" {
			behind = r + ": " + msg
		}
		hb := hv.Status.LastHeartbeat
		return hv.Ready() && hb != nil && time.Since(*hb) < 2*coreHeartbeat, fmt.Sprintf("ready=%v hb=%v behind=%s", hv.Ready(), hb, st)
	})
	time.Sleep(time.Minute) // a resync or two through x
	hv := h.hosts(t)["w1"]
	if st, r, msg := condition(hv, "ControlPlaneBehind"); st == "True" || behind != "" {
		t.Errorf("c: K-071: w1 held, talking to %s (healthy, back from being off): %s %s %s %s", x, st, r, msg, behind)
	} else {
		t.Logf("c: w1 did not hold talking to %s first (ControlPlaneBehind %q)", x, st)
	}
	j, _ := h.sh("w1", "journalctl -u kevasod --no-pager --since '"+since+"' | grep -iE 'behind|hold|older' | grep -v heartbeat | tail -5")
	if strings.TrimSpace(j) != "" {
		t.Logf("c: w1's journal on holds since %s:\n%s", since, j)
	}
	h.uncut("w1")
	if got := h.one(t, "w1", ss["base"].Metadata.UID); got != baseBefore {
		t.Errorf("c: base restarted on w1: %s, was %s", got, baseBefore)
	}
	h.waitRunning(t, ns, 3*time.Minute, "base", "while-1", "while-2")
	if l := leaders(h.hosts(t), cps...); len(l) != 1 {
		t.Errorf("c: leaders after %s rejoined: %v", x, l)
	}
}

// ── d: kvoadm upgrade across all five ─────────────────────────────────────────────────────────

// unitProc is a unit's main process: PID, start (monotonic) and its program, as the kernel has it
// ("(deleted)" when the file it started from was replaced since).
func (h *ha3) unitProc(m, unit string) string {
	out, _ := h.sh(m, "p=$(systemctl show -p MainPID --value "+unit+" 2>/dev/null); [ -n \"$p\" ] && [ \"$p\" != 0 ] && "+
		"echo $p $(systemctl show -p ActiveEnterTimestampMonotonic --value "+unit+") $(readlink /proc/$p/exe) || echo none")
	return strings.TrimSpace(out)
}

// ha3Upgrade (d): kvoadm upgrade across the five machines with silos running on both workers (one
// opted in, so their fence watchers are armed): none restarted, the API answers throughout, and
// on each machine with a fence the watcher was replaced by the new program.
func ha3Upgrade(t *testing.T, h *ha3) {
	const ns = "ha-d"
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "a", "w1")+"---\n"+siloYAML(ns, "b", "w2")+"---\n"+
		siloYAML(ns, "f1", "w1", "availability: { onControlPlaneLoss: Stop, after: 60s }")+"---\n"+
		siloYAML(ns, "f2", "w2", "availability: { onControlPlaneLoss: Stop, after: 60s }"))
	names := []string{"a", "b", "f1", "f2"}
	ss := h.waitRunning(t, ns, 10*time.Minute, names...)
	before := map[string]string{}
	for n, s := range ss {
		before[n] = h.one(t, s.Status.Host, s.Metadata.UID)
	}
	hs := h.hosts(t)
	cur := hs["cp1"].Status.Version
	next := nextPatch(t, cur)
	to := h.kevasod(next)
	all := []string{"cp1", "cp2", "cp3", "w1", "w2"}
	fenceBefore, agentBefore := map[string]string{}, map[string]string{}
	for _, n := range all {
		fenceBefore[n], agentBefore[n] = h.unitProc(n, "kevaso-fence"), h.unitProc(n, "kevasod")
	}
	t.Logf("d: %s → %s; fence watchers before: %v", cur, next, fenceBefore)
	var okN, fails atomic.Int64
	var failed sync.Map
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			if out, err := h.c.Kvo("", "get", "silos", "-n", ns); err == nil {
				okN.Add(1)
			} else {
				fails.Add(1)
				failed.Store(time.Now().Format(time.TimeOnly), firstLine(out))
			}
		}
	}()
	start := time.Now()
	out, err := h.kvoadm("", "upgrade", "--kevasod", to, "--ssh-key", h.b.Key, "--version", next)
	close(stop)
	wg.Wait()
	t.Logf("d: kvoadm upgrade (%s):\n%s", time.Since(start).Round(time.Second), out)
	if err != nil {
		t.Fatalf("d: kvoadm upgrade: %v", err)
	}
	t.Logf("d: the API was asked %d times during the roll: %d failed", okN.Load()+fails.Load(), fails.Load())
	if fails.Load() > 0 {
		var f []string
		failed.Range(func(k, v any) bool { f = append(f, fmt.Sprint(k, " ", v)); return true })
		sort.Strings(f)
		t.Errorf("d: the API failed during the upgrade: %s", strings.Join(f, "; "))
	}
	eventually(t, "every host at "+next+" and Ready", 5*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		var s []string
		ok := true
		for _, n := range all {
			s = append(s, n+"="+hs[n].Status.Version)
			ok = ok && hs[n].Status.Version == next && hs[n].Ready()
		}
		return ok, strings.Join(s, " ")
	})
	for n, s := range ss {
		if got := h.one(t, s.Status.Host, s.Metadata.UID); got != before[n] {
			t.Errorf("d: %s restarted in the upgrade: %s, was %s", n, got, before[n])
		}
	}
	for _, n := range all {
		f, a := h.unitProc(n, "kevaso-fence"), h.unitProc(n, "kevasod")
		if a == agentBefore[n] || strings.Contains(a, "(deleted)") {
			t.Errorf("d: %s's kevasod not restarted on the new program: %s (was %s)", n, a, agentBefore[n])
		}
		switch {
		case fenceBefore[n] == "none" && f == "none":
			t.Logf("d: %s has no fence watcher", n)
		case f == "none":
			t.Errorf("d: %s's fence watcher is gone after the upgrade (was %s)", n, fenceBefore[n])
		case f == fenceBefore[n] || strings.Contains(f, "(deleted)"):
			t.Errorf("d: %s's fence watcher was not replaced: %s (was %s)", n, f, fenceBefore[n])
		default:
			t.Logf("d: %s's fence watcher replaced: %s → %s", n, fenceBefore[n], f)
		}
	}
	for _, w := range []string{"w1", "w2"} {
		if fenceBefore[w] == "none" {
			t.Errorf("d: %s runs an opted-in silo and had no fence watcher before the upgrade", w)
		}
	}
}

// nextPatch is the version one patch above v (vMAJOR.MINOR.PATCH, maybe with a suffix).
func nextPatch(t *testing.T, v string) string {
	m := regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)`).FindStringSubmatch(v)
	if m == nil {
		t.Fatalf("version %q isn't vX.Y.Z", v)
	}
	p, _ := strconv.Atoi(m[3])
	return fmt.Sprintf("v%s.%s.%d", m[1], m[2], p+1)
}

// ── e: a system node whose data directory is wiped ────────────────────────────────────────────

// ha3Wipe (e): a system node (not the leader) stopped, its /var/lib/kevaso wiped, the machine
// powered off and on: kevasod must not start an empty cluster, and must say what to do. Then it
// rejoins the proper way: removed from the cluster, reset, added again.
func ha3Wipe(t *testing.T, h *ha3) {
	const ns = "ha-e"
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "steady", "w2"))
	ss := h.waitRunning(t, ns, 10*time.Minute, "steady")
	steady := h.one(t, "w2", ss["steady"].Metadata.UID)
	lead := h.leader(t)
	y := others(lead)[0]
	hs := h.hosts(t)
	version := hs[y].Status.Version
	t.Logf("e: wiping %s (leader %s, version %s)", y, lead, version)
	h.mustSh(t, y, "systemctl stop kevasod && rm -rf /var/lib/kevaso/* && ls -A /var/lib/kevaso | wc -l")
	h.b.PowerOff(h.ctx, h.ms[y])
	h.b.PowerOn(h.ctx, h.ms[y])
	bootAt := time.Now()
	time.Sleep(90 * time.Second)
	state, _ := h.sh(y, "systemctl is-active kevasod; systemctl show -p NRestarts --value kevasod; ls -A /var/lib/kevaso /var/lib/kevaso/pki 2>&1 | head; ls /var/lib/kevaso/etcd/data/member 2>&1 | head -2")
	journal, _ := h.sh(y, "journalctl -u kevasod -b --no-pager | grep -v heartbeat | tail -25")
	t.Logf("e: %s, %s after boot: kevasod %s\njournal:\n%s", y, time.Since(bootAt).Round(time.Second), strings.TrimSpace(state), journal)
	refused := regexp.MustCompile(`(?i)not starting|refus|belongs to`).FindString(journal) != ""
	says := strings.Contains(journal, "kvoadm")
	if !refused {
		t.Errorf("e: %s with a wiped data directory: no refusal in its journal", y)
	}
	if !says {
		t.Errorf("e: %s's refusal doesn't say what to do (no kvoadm command in it)", y)
	}
	if strings.Contains(state, "member") && !strings.Contains(state, "No such file") {
		t.Errorf("e: %s made a store after the wipe: %s", y, state)
	}
	// Nothing new served: the API at y doesn't answer with the cluster (or at all).
	if out, err := h.c.KvoAt(h.url(y), "get", "hosts"); err == nil {
		t.Errorf("e: %s serves the API after its data was wiped: %s", y, firstLine(out))
	}
	hs = h.hosts(t)
	if hs[y].Ready() {
		t.Errorf("e: %s reads Ready after the wipe", y)
	}
	if l := leaders(hs, others(y)...); len(l) != 1 {
		t.Errorf("e: leaders of the other two: %v", l)
	}
	if hv := hs["w2"]; !hv.Ready() {
		t.Errorf("e: w2 not Ready while %s is wiped", y)
	}
	if got := h.one(t, "w2", ss["steady"].Metadata.UID); got != steady {
		t.Errorf("e: steady restarted: %s, was %s", got, steady)
	}
	t.Logf("e: %s refused (%v), said what to do (%v); the others lead and serve", y, refused, says)

	// Rejoin the proper way.
	out := h.mustKvoadm("", "host", "remove", y)
	t.Logf("e: kvoadm host remove %s:\n%s", y, out)
	out = h.reset(t, y)
	t.Logf("e: kvoadm reset %s:\n%s", y, out)
	out = h.mustKvoadm("", append([]string{"host", "add", "root@" + h.ms[y].Public, "--external-ip", h.ms[y].Public, "--kevasod", h.kevasod(version), "--name", y,
		"--install", "control-plane"}, h.ssh...)...)
	t.Logf("e: kvoadm host add %s --install control-plane:\n%s", y, out)
	h.healthy(t)
	if out, err := h.c.KvoAt(h.url(y), "get", "silo", "steady", "-n", ns); err != nil || !strings.Contains(out, "Running") {
		t.Errorf("e: %s after rejoining: %v %s", y, err, out)
	}
	if got := h.one(t, "w2", ss["steady"].Metadata.UID); got != steady {
		t.Errorf("e: steady restarted: %s, was %s", got, steady)
	}
	t.Logf("e: %s removed, reset and added again: a voter, serving; steady untouched", y)
}

// ── f: a restore from a backup, with a silo made after it ─────────────────────────────────────

// ha3Restore (f): a backup taken; a silo with a volume made after it on w1; the three system
// nodes reset and the cluster restored from the backup (kvoadm init --restore on cp1, the other two
// added again). The workers take the restored control plane as a new history (no hold); the silo
// made after the backup, which the restored state doesn't know, keeps running on w1 with its
// volume, and w1 lists it as kept.
func ha3Restore(t *testing.T, h *ha3) {
	const ns = "ha-f"
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "old", "w2"))
	ss := h.waitRunning(t, ns, 10*time.Minute, "old")
	oldC := h.one(t, "w2", ss["old"].Metadata.UID)
	key, err := os.ReadFile(h.recv)
	if err != nil {
		t.Skipf("f: no recovery key kept for this cluster (%v): it needs a run that made the cluster", err)
	}
	lead := h.leader(t)
	file := filepath.Join(t.TempDir(), "ha3.kvb")
	t.Log(h.mustKvoadm("", append([]string{"backup", "root@" + h.ms[lead].Public, "--output", file}, h.ssh...)...))

	// After the backup: a silo with a volume, on w1.
	h.apply(t, siloYAML(ns, "late", "w1", "volumes: [{ name: data, size: 1Gi, mountPath: /data }]"))
	late := h.waitRunning(t, ns, 10*time.Minute, "late")["late"]
	h.c.MustKvo("", "exec", "silo", "late", "-n", ns, "--", "sh", "-c", "echo kept > /data/marker && sync")
	lateC := h.one(t, "w1", late.Metadata.UID)
	// The volume as the container has it mounted (a docker volume for the docker runtime).
	volDir := strings.TrimSpace(h.mustSh(t, "w1", "docker inspect $(docker ps -q --filter label=kevaso.com/uid="+late.Metadata.UID+
		" --filter label=kevaso.com/name | head -1) -f '{{range .Mounts}}{{if eq .Destination \"/data\"}}{{.Source}}{{end}}{{end}}'"))
	if out := h.mustSh(t, "w1", "cat "+volDir+"/marker 2>&1"); volDir == "" || !strings.Contains(out, "kept") {
		t.Fatalf("f: late's volume on w1 (%q) doesn't hold the marker: %s", volDir, out)
	}
	t.Logf("f: late's volume on w1 is %s", volDir)
	hs := h.hosts(t)
	version := hs["cp1"].Status.Version
	kevasod := h.kevasod(version)

	// The three system nodes reset (the cluster's control plane is gone), then cp1 restored.
	for _, n := range cps {
		out := h.reset(t, n)
		t.Logf("f: kvoadm reset %s: %s", n, lastLine(out))
	}
	m := h.ms["cp1"]
	start := time.Now()
	out := h.mustKvoadm(string(key), append([]string{"init", "root@" + m.Public, "--restore", file, "--store", "etcd", "--kevasod", kevasod, "--name", "cp1", "--cluster", "ha3",
		"--external-ip", m.Public, "--api-server", h.url("cp1")}, h.ssh...)...)
	t.Logf("f: kvoadm init --restore (%s):\n%s", time.Since(start).Round(time.Second), redact(out))
	for _, w := range []string{"w1", "w2"} {
		if !strings.Contains(out, "worker "+w+" reconnected") {
			t.Errorf("f: the restore didn't say it reconnected %s", w)
		}
	}
	eventually(t, "w1 and w2 Ready with the restored control plane", 10*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		return hs["w1"].Ready() && hs["w2"].Ready(), fmt.Sprintf("w1=%v w2=%v", hs["w1"].Ready(), hs["w2"].Ready())
	})
	hs = h.hosts(t)
	for _, w := range []string{"w1", "w2"} {
		if st, r, msg := condition(hs[w], "ControlPlaneBehind"); st == "True" {
			t.Errorf("f: %s holds after a deliberate restore: %s %s", w, r, msg)
		}
	}
	if o, err := h.c.Kvo("", "get", "silo", "late", "-n", ns); err == nil {
		t.Fatalf("f: the restored state knows the silo made after the backup: %s", o)
	}
	time.Sleep(time.Minute) // a resync or two on the workers
	if got := h.one(t, "w1", late.Metadata.UID); got != lateC {
		t.Errorf("f: late restarted on w1: %s, was %s", got, lateC)
	}
	if out, _ := h.sh("w1", "cat "+volDir+"/marker 2>&1"); !strings.Contains(out, "kept") {
		t.Errorf("f: late's volume on w1 lost its data: %s", out)
	}
	if got := h.one(t, "w2", ss["old"].Metadata.UID); got != oldC {
		t.Errorf("f: old (in the backup) restarted on w2: %s, was %s", got, oldC)
	}
	eventually(t, "w1 lists late as kept", 3*time.Minute, func() (bool, string) {
		st, _, msg := condition(h.hosts(t)["w1"], "KeptSilos")
		return st == "True" && strings.Contains(msg, ns+"/late"), st + " " + msg
	})
	_, _, kept := condition(h.hosts(t)["w1"], "KeptSilos")
	t.Logf("f: workers took the restore as a new history; late kept running on w1 with its volume's data; w1 says: %s", kept)
	j, _ := h.sh("w1", "journalctl -u kevasod --no-pager --since '-15min' | grep -i keeping | tail -3")
	t.Logf("f: w1's journal: %s", strings.TrimSpace(j))

	// The other two system nodes, added again: the cluster is whole.
	for _, n := range []string{"cp2", "cp3"} {
		if out, err := h.kvoadm("", "host", "remove", n); err != nil {
			t.Logf("f: kvoadm host remove %s (the restored state's): %v %s", n, err, out)
		}
		t.Log(h.mustKvoadm("", append([]string{"host", "add", "root@" + h.ms[n].Public, "--external-ip", h.ms[n].Public, "--kevasod", kevasod, "--name", n,
			"--install", "control-plane"}, h.ssh...)...))
	}
	h.healthy(t)
	t.Log("f: cp2 and cp3 added again: three voters")
}
