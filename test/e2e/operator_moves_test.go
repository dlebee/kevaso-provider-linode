//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── 4. Plugin operators ────────────────────────────────────────────────────────────────────────

func (c *cluster) operator(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	pkg := filepath.Join(filepath.Dir(file), "..", "..", "..", "core", "examples", "operator-session")
	// On a reused cluster an earlier run may have left the plugin installed (or half: its
	// controller never started); installing again takes over its own objects.
	if out, _ := c.sh("", c.kvoadm, "plugin", "list"); strings.Contains(out, "sessions") {
		t.Logf("sessions is already installed (an earlier run):\n%s", out)
		c.fresh(t, "session", "dev")
		c.fresh(t, "sessiontemplate", "py")
	}
	bin := build(t, c.ctx, t.TempDir(), "kevaso-operator-session", "kevaso.com/core/examples/operator-session/cmd/kevaso-operator-session", true)
	out := c.must(t, "", c.kvoadm, append([]string{"plugin", "install", "root@" + c.cp.Public, "--package", pkg, "--binary", bin,
		"--api-server", "https://" + c.cp.Public + ":6443", "--yes"}, c.ssh...)...)
	t.Logf("kvoadm plugin install --package:\n%s", out)
	for _, w := range []string{"sessions", "kevaso-system/plugin-sessions", "silos"} {
		if !strings.Contains(out, w) {
			t.Fatalf("the install summary doesn't show %q", w)
		}
	}
	const unit = "kevaso-plugin-sessions.service"
	if out, err := c.b.ssh(c.ctx, c.cp, "systemctl is-active "+unit); err != nil || strings.TrimSpace(out) != "active" {
		t.Fatalf("%s: %s %v", unit, out, err)
	}

	// Sandboxed as declared: a dynamic user, no capabilities, no new privileges, the file
	// system read-only but for its state, and its credentials through systemd only.
	props, _ := c.b.ssh(c.ctx, c.cp, "systemctl show "+unit+" -p DynamicUser -p NoNewPrivileges -p ProtectSystem -p ProtectHome -p PrivateTmp "+
		"-p PrivateDevices -p CapabilityBoundingSet -p RestrictNamespaces -p MemoryDenyWriteExecute -p StateDirectory -p MainPID; systemctl cat "+unit+" | grep LoadCredential")
	for _, w := range []string{"DynamicUser=yes", "NoNewPrivileges=yes", "ProtectSystem=strict", "ProtectHome=yes", "PrivateTmp=yes",
		"PrivateDevices=yes", "CapabilityBoundingSet=\n", "MemoryDenyWriteExecute=yes", "StateDirectory=kevaso-plugin-sessions", "LoadCredential=token.json:/etc/kevaso/plugins/sessions/token.json"} {
		if !strings.Contains(props+"\n", w) {
			t.Fatalf("%s: no %q in\n%s", unit, w, props)
		}
	}
	pid := ""
	for _, l := range strings.Split(props, "\n") {
		if v, ok := strings.CutPrefix(l, "MainPID="); ok {
			pid = strings.TrimSpace(v)
		}
	}
	st, _ := c.b.ssh(c.ctx, c.cp, "grep -E '^(Uid|NoNewPrivs|CapEff|CapPrm):' /proc/"+pid+"/status; tr '\\0' '\\n' < /proc/"+pid+"/environ | grep -c -i token; "+
		"nsenter -t "+pid+" -m sh -c 'touch /etc/kevaso-x 2>&1; touch /usr/local/bin/x 2>&1; ls /root 2>&1' || true")
	for _, w := range []string{"NoNewPrivs:\t1", "CapEff:\t0000000000000000", "CapPrm:\t0000000000000000"} {
		if !strings.Contains(st, w) {
			t.Fatalf("controller process %s: no %q in\n%s", pid, w, st)
		}
	}
	if strings.Contains(st, "Uid:\t0\t") || !strings.Contains(st, "Read-only file system") {
		t.Fatalf("controller process %s: root, or a writable system:\n%s", pid, st)
	}
	t.Logf("the controller runs sandboxed:\n%s\n%s", props, st)

	// A Session gets its silo and a status.
	c.apply(t, `apiVersion: example.dev/v1
kind: SessionTemplate
metadata: { name: py }
spec:
  silo:
    runtime: docker
    image: `+pyImage+`
    resources: { memory: 64Mi }
    command: [python3, -c, "import time\nwhile True: time.sleep(3600)"]
---
apiVersion: example.dev/v1
kind: Session
metadata: { name: dev }
spec:
  template: py
  prompt: prove plugin operators on real hosts
`)
	c.eventually(t, "session dev running with its silo", 6*time.Minute, func() (bool, string) {
		out, err := c.kvoe("get", "session", "dev", "-o", "json")
		var s struct {
			Status struct {
				Phase    string `json:"phase"`
				Replicas int    `json:"replicas"`
				Silos    []struct{ Name, Phase string }
			} `json:"status"`
		}
		if err != nil || json.Unmarshal([]byte(out), &s) != nil {
			return false, out
		}
		return s.Status.Phase == "Running" && s.Status.Replicas == 1 && len(s.Status.Silos) == 1 && s.Status.Silos[0].Name == "dev-0" &&
			s.Status.Silos[0].Phase == "Running", fmt.Sprintf("%+v", s.Status)
	})
	if out := c.mustKvo(t, "get", "sessions"); !strings.Contains(out, "dev") {
		t.Fatalf("kvo get sessions:\n%s", out)
	}
	t.Log("session dev made silo dev-0 and reports it Running")

	// Its token holds its own roles only.
	tok, err := c.b.ssh(c.ctx, c.cp, "cat /var/lib/private/kevaso-plugin-sessions/token.json 2>/dev/null || cat /etc/kevaso/plugins/sessions/token.json")
	var tk struct{ Token string }
	if err != nil || json.Unmarshal([]byte(tok), &tk) != nil || tk.Token == "" {
		t.Fatal("the controller's token isn't where its unit keeps it")
	}
	asPlugin := func(args ...string) (string, error) {
		out, err := c.cmd("", []string{"KVO_TOKEN=" + tk.Token}, c.kvo, args...).CombinedOutput()
		return string(out), err
	}
	if out, err := asPlugin("get", "sessions", "-n", "e2e"); err != nil {
		t.Fatalf("as the plugin, its own kind: %v %s", err, out)
	}
	if out, err := asPlugin("get", "silos", "-n", "e2e"); err != nil {
		t.Fatalf("as the plugin, silos: %v %s", err, out)
	}
	for _, try := range []struct {
		stdin string
		args  []string
	}{
		{"", []string{"get", "secrets", "-n", "kevaso-system"}},
		{"", []string{"get", "hosts"}},
		{"", []string{"exec", "silo", "dev-0", "-n", "e2e", "--", "true"}},
		{"apiVersion: kevaso/v1\nkind: ClusterRoleBinding\nmetadata: { name: escalate }\nroleRef: { kind: ClusterRole, name: cluster-admin }\n" +
			"subjects: [{ kind: ServiceAccount, name: plugin-sessions, namespace: kevaso-system }]\n", []string{"apply", "-f", "-"}},
	} {
		x := c.cmd(try.stdin, []string{"KVO_TOKEN=" + tk.Token}, c.kvo, try.args...)
		out, err := x.CombinedOutput()
		if err == nil || !strings.Contains(strings.ToLower(string(out)), "forbidden") && !strings.Contains(string(out), "403") {
			t.Fatalf("as the plugin, kvo %s: allowed or not a refusal: %v %s", strings.Join(try.args, " "), err, out)
		}
	}
	t.Log("the controller's token: its kinds and silos yes; secrets, hosts, exec and making itself cluster-admin refused")

	// kvoadm plugin list shows it running.
	if out := c.must(t, "", c.kvoadm, "plugin", "list"); !strings.Contains(out, "sessions") || !strings.Contains(out, "running") {
		t.Fatalf("kvoadm plugin list:\n%s", out)
	} else {
		t.Logf("kvoadm plugin list:\n%s", out)
	}

	// Uninstall: refused while sessions exist; works after; the unit and the kinds go.
	if out, err := c.sh("", c.kvoadm, append([]string{"plugin", "uninstall", "sessions"}, c.ssh...)...); err == nil || !strings.Contains(out, "still have objects") {
		t.Fatalf("uninstall with a session left: %v %s", err, out)
	}
	c.mustKvo(t, "delete", "session", "dev")
	c.eventually(t, "dev-0 deleted with its session", 3*time.Minute, func() (bool, string) {
		_, err := c.silo("dev-0")
		_, serr := c.kvoe("get", "session", "dev")
		return err != nil && serr != nil, fmt.Sprint(err, serr)
	})
	c.mustKvo(t, "delete", "sessiontemplate", "py")
	t.Log(c.must(t, "", c.kvoadm, append([]string{"plugin", "uninstall", "sessions"}, c.ssh...)...))
	if out, _ := c.b.ssh(c.ctx, c.cp, "systemctl is-active "+unit+"; ls /etc/systemd/system/"+unit+" /etc/kevaso/plugins/sessions 2>&1"); strings.Contains(out, "\nactive") || !strings.Contains(out, "No such file") {
		t.Fatalf("after uninstall, on the host:\n%s", out)
	}
	if out, _ := c.sh("", c.kvoadm, "plugin", "list"); strings.Contains(out, "sessions") {
		t.Fatalf("still listed:\n%s", out)
	}
	if out, err := c.kvoe("get", "sessions"); err == nil {
		t.Fatalf("the Session kind outlived its plugin: %s", out)
	}
	t.Log("uninstall refused while a session existed; afterwards it removed the unit, the files and the kinds")
}

// ── 2. Moves ───────────────────────────────────────────────────────────────────────────────────

// watcher samples, on each worker, how many running containers a silo has there.
type watcher struct {
	mu      sync.Mutex
	samples map[string][]sample // host → samples
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type sample struct {
	at time.Time // the host's clock (NTP on Linodes)
	n  int
}

func (c *cluster) watch(t *testing.T, silo string) *watcher {
	ctx, cancel := context.WithCancel(c.ctx)
	w := &watcher{samples: map[string][]sample{}, cancel: cancel}
	for _, name := range []string{"w1", "w2"} {
		m := c.host(name)
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			cmd := c.b.sshCmd(ctx, m, "while :; do echo $(date +%s.%N) $(docker ps -q --filter label=kevaso.com/namespace=e2e --filter label=kevaso.com/name="+
				silo+" --filter status=running | wc -l); sleep 0.25; done")
			out, err := cmd.StdoutPipe()
			if err != nil || cmd.Start() != nil {
				t.Errorf("watch %s: %v", name, err)
				return
			}
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				f := strings.Fields(sc.Text())
				if len(f) != 2 {
					continue
				}
				sec, err1 := strconv.ParseFloat(f[0], 64)
				n, err2 := strconv.Atoi(f[1])
				if err1 != nil || err2 != nil {
					continue
				}
				w.mu.Lock()
				w.samples[name] = append(w.samples[name], sample{time.Unix(0, int64(sec*1e9)), n})
				w.mu.Unlock()
			}
			_ = cmd.Wait()
		}()
	}
	return w
}

// stop ends the watch and fails if both hosts ever ran a copy at once: the last time the one
// left ran it must be before the first time the other did. Returns the gap.
func (w *watcher) stop(t *testing.T, from, to string) time.Duration {
	t.Helper()
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	both := map[string][]sample{}
	for h, ss := range w.samples {
		both[h] = append([]sample(nil), ss...)
		sort.Slice(both[h], func(i, j int) bool { return both[h][i].at.Before(both[h][j].at) })
	}
	if len(both[to]) == 0 {
		t.Fatalf("no samples from %s", to)
	}
	var lastFrom, firstTo time.Time
	for _, s := range both[from] {
		if s.n > 0 {
			lastFrom = s.at
		}
	}
	for _, s := range both[to] {
		if s.n > 0 {
			firstTo = s.at
			break
		}
	}
	for _, ss := range both {
		for _, s := range ss {
			if s.n > 1 {
				t.Fatalf("two copies on one host at %s", s.at)
			}
		}
	}
	if firstTo.IsZero() {
		t.Fatalf("it never ran on %s while watched (%d samples)", to, len(both[to]))
	}
	if !lastFrom.IsZero() && !lastFrom.Before(firstTo) {
		t.Fatalf("two copies: still running on %s at %s, already on %s at %s", from, lastFrom.Format("15:04:05.000"), to, firstTo.Format("15:04:05.000"))
	}
	if lastFrom.IsZero() {
		return 0
	}
	return firstTo.Sub(lastFrom)
}

func (c *cluster) moves(t *testing.T) {
	c.fresh(t, "silo", "roam", "keep", "ha")
	// A docker silo with nothing local moves anywhere: the old host stops it first.
	c.apply(t, `apiVersion: kevaso/v1
kind: Silo
metadata: { name: roam }
spec:
  runtime: docker
  image: `+pyImage+`
  resources: { memory: 64Mi }
  command: [python3, -c, "import time\nwhile True: time.sleep(3600)"]
`)
	roam := c.running(t, "roam", 6*time.Minute)
	if roam.Status.Movable != "Anywhere" {
		t.Errorf("roam's movable: %q, want Anywhere", roam.Status.Movable)
	}
	c.moveBy(t, "roam", roam.Status.Host, other(roam.Status.Host))

	// A silo on a linode-block volume moves within the region, and its file with it.
	vol := func(name, policy string) string {
		return fmt.Sprintf(`apiVersion: kevaso/v1
kind: Silo
metadata: { name: %s }
spec:
  runtime: docker
  image: %s
  resources: { memory: 64Mi }
  command: [python3, -c, "import time\nwhile True: time.sleep(3600)"]
  availability: { onHostLoss: %s }
  volumes:
    - { name: data, class: linode-block, size: 10Gi, mountPath: /data }
`, name, pyImage, policy)
	}
	c.apply(t, vol("keep", "Wait"))
	c.apply(t, vol("ha", "Move"))
	proof := map[string]string{}
	for _, name := range []string{"keep", "ha"} {
		s := c.running(t, name, 10*time.Minute)
		if !strings.HasPrefix(s.Status.Movable, "Region") {
			t.Errorf("%s's movable: %q, want Region (…)", name, s.Status.Movable)
		}
		r := make([]byte, 8)
		_, _ = rand.Read(r)
		proof[name] = hex.EncodeToString(r)
		c.mustKvo(t, "exec", "silo", name, "--", "sh", "-c", "echo "+proof[name]+" > /data/proof && sync")
	}
	keep, _ := c.silo("keep")
	hasProof := func(name string) {
		t.Helper()
		if out := c.mustKvo(t, "exec", "silo", name, "--", "cat", "/data/proof"); strings.TrimSpace(out) != proof[name] {
			t.Fatalf("%s's file after the move: %q, want %s", name, out, proof[name])
		}
	}
	c.moveBy(t, "keep", keep.Status.Host, other(keep.Status.Host))
	hasProof("keep")
	t.Log("keep moved with its linode-block volume; its file is there")

	// keep and ha on one host, H; H powered off through the Linode API: ha (onHostLoss: Move) comes
	// up on the other worker with its file once fenced; keep (Wait) moves only by hand, and only
	// with --confirm-fenced.
	keep, _ = c.silo("keep")
	ha, _ := c.silo("ha")
	h := keep.Status.Host
	if ha.Status.Host != h {
		c.moveBy(t, "ha", ha.Status.Host, h)
		hasProof("ha")
	}
	o := other(h)
	w := c.watch(t, "ha")
	time.Sleep(3 * time.Second)
	if err := c.b.api.ShutdownInstance(c.ctx, c.host(h).ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { // back on, for whatever runs next on this cluster
		if err := c.b.api.BootInstance(context.Background(), c.host(h).ID, 0); err != nil {
			t.Logf("boot %s again: %v", h, err)
			return
		}
		c.eventually(t, h+" Ready again", 6*time.Minute, func() (bool, string) {
			out, _ := c.sh("", c.kvo, "get", "host", h)
			return strings.Contains(out, "Ready") && !strings.Contains(out, "NotReady"), firstLine(out)
		})
		t.Logf("%s is back", h)
	})
	down := time.Now()
	t.Logf("%s (Linode %d) powered off", h, c.host(h).ID)
	sawFencing := false
	c.eventually(t, "ha running on "+o+" after losing "+h, 8*time.Minute, func() (bool, string) {
		s, err := c.silo("ha")
		if _, r := s.cond("HostLost"); r == "Fencing" {
			sawFencing = true
		}
		return err == nil && s.Status.Host == o && s.Status.Phase == "Running", fmt.Sprintf("%+v %v", s.Status, err)
	})
	gap := w.stop(t, h, o)
	if !sawFencing {
		t.Error("ha never showed HostLost/Fencing before it moved")
	}
	hasProof("ha")
	ha, _ = c.silo("ha")
	if _, r := ha.cond("HostLost"); r != "Moved" {
		t.Errorf("ha's HostLost reason %q, want Moved", r)
	}
	t.Logf("ha came up on %s with its file %s after %s was powered off (%s between copies), after fencing", o, time.Since(down).Round(time.Second), h, gap.Round(time.Millisecond))

	keep, _ = c.silo("keep")
	if keep.Status.Host != h || keep.Status.Phase != "Unknown" {
		t.Fatalf("keep (onHostLoss: Wait) should stay on lost %s: %+v", h, keep.Status)
	}
	if out, err := c.kvoe("move", "silo", "keep", "--to", o); err == nil || !strings.Contains(out, "--confirm-fenced") {
		t.Fatalf("a move off the lost host without --confirm-fenced: %v %s", err, out)
	}
	out := c.mustKvo(t, "move", "silo", "keep", "--to", o, "--confirm-fenced")
	t.Logf("kvo move --confirm-fenced: %s", strings.TrimSpace(out))
	c.eventually(t, "keep running on "+o, 8*time.Minute, func() (bool, string) {
		s, err := c.silo("keep")
		return err == nil && s.Status.Host == o && s.Status.Phase == "Running", fmt.Sprintf("%+v %v", s.Status, err)
	})
	hasProof("keep")
	t.Logf("keep: refused off lost %s without --confirm-fenced; with it, up on %s with its file", h, o)
	for _, name := range []string{"roam", "keep", "ha"} {
		c.mustKvo(t, "delete", "silo", name)
	}
	c.eventually(t, "the volumes deleted with their silos", 6*time.Minute, func() (bool, string) {
		var left []string
		for _, l := range c.b.remaining(c.ctx) {
			if strings.HasPrefix(l, "volume ") {
				left = append(left, l)
			}
		}
		return len(left) == 0, strings.Join(left, ", ")
	})
}

// moveBy moves a silo by hand between two live hosts, watching both: never two copies, and
// status.movingFrom says the old host is stopping it, then clears.
func (c *cluster) moveBy(t *testing.T, name, from, to string) {
	t.Helper()
	w := c.watch(t, name)
	time.Sleep(2 * time.Second) // a few samples of before
	start := time.Now()
	out := c.mustKvo(t, "move", "silo", name, "--to", to)
	if !strings.Contains(out, "moving to "+to) {
		t.Fatalf("kvo move: %s", out)
	}
	sawMovingFrom := strings.Contains(out, from+" stops it first")
	c.eventually(t, name+" running on "+to, 8*time.Minute, func() (bool, string) {
		s, err := c.silo(name)
		if s.Status.MovingFrom == from {
			sawMovingFrom = true
		}
		return err == nil && s.Status.Host == to && s.Status.Phase == "Running" && s.Status.MovingFrom == "", fmt.Sprintf("%+v %v", s.Status, err)
	})
	time.Sleep(2 * time.Second)
	gap := w.stop(t, from, to)
	if !sawMovingFrom {
		t.Errorf("%s: never saw status.movingFrom %s", name, from)
	}
	t.Logf("%s moved %s → %s in %s, never two copies (%s between them)", name, from, to, time.Since(start).Round(time.Second), gap.Round(time.Millisecond))
}
