//go:build e2e

// TestHA is M16's "done when" on real Linodes: system nodes joined with kvoadm, one of them lost
// and back, two plus a light node, a clean removal, a rolling upgrade, and a cluster moved from
// SQLite to etcd. Linodes have no KVM, so the silos are containers (docker).
//
//	KEVASO_LINODE_TOKEN_FILE=~/linode-token.txt go test -tags e2e -v -run TestHA -timeout 180m ./test/e2e/
//
// Two parts, one after the other so that at most four Linodes exist at once (KEVASO_E2E_HA_PART=
// three or light runs one): "three" (three system nodes and a worker: steps 1, 2, 3, 6, 5) and
// "light" (a cluster started on SQLite and migrated, then two system nodes and a light one: steps
// 7 and 4). Everything is labelled and tagged kvt-ha-RUN and swept at the end.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHA(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-ha-")
	t.Logf("run %s, region %s", b.tag, b.region)
	b.cleanupAtEnd(t)
	k := newKit(t, ctx)
	// Two builds of the same code, told apart by their version only (step 6 rolls one to the other).
	from := k.build("kevasod-v0.16.0", "kevaso.com/core/cmd/kevasod", "linux", "v0.16.0")
	to := k.build("kevasod-v0.16.1", "kevaso.com/core/cmd/kevasod", "linux", "v0.16.1")
	part := os.Getenv("KEVASO_E2E_HA_PART")
	if part == "" || part == "three" {
		haThree(t, ctx, b, k, from, to)
	}
	if t.Failed() {
		return
	}
	if part == "" || part == "light" {
		haLight(t, ctx, b, k, from)
	}
}

// ha is one part's cluster on its machines.
type ha struct {
	t  *testing.T
	b  *bed
	c  *cluster
	ms map[string]machine
	ns string
}

func newHA(t *testing.T, ctx context.Context, b *bed, k *kit, ns string, roles ...string) *ha {
	h := &ha{t: t, b: b, c: k.cluster(), ms: b.createAll(ctx, roles...), ns: ns}
	t.Cleanup(func() { // before the sweep: what was going on, if it failed
		if !t.Failed() {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		for _, r := range roles {
			out, _ := b.ssh(dctx, h.ms[r], "systemctl is-active kevasod; grep ExecStart /etc/systemd/system/kevasod.service; "+
				"journalctl -u kevasod -n 120 --no-pager 2>&1 | grep -iv heartbeat | tail -80")
			t.Logf("── %s ──\n%s", r, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "yaml"}, {"get", "silos", "-A", "-o", "wide"}} {
			c := exec.CommandContext(dctx, k.kvo, args...)
			c.Env = h.c.env
			out, _ := c.CombinedOutput()
			t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})
	return h
}

// done deletes the part's machines and waits until the account no longer lists them.
func (h *ha) done(ctx context.Context) {
	for _, m := range h.ms {
		h.b.destroy(ctx, m)
	}
	eventually(h.t, "the part's Linodes gone", 5*time.Minute, func() (bool, string) {
		var left []string
		for _, in := range h.b.instances(ctx) {
			for _, m := range h.ms {
				if in.ID == m.ID {
					left = append(left, in.Label)
				}
			}
		}
		return len(left) == 0, strings.Join(left, ", ")
	})
}

func (h *ha) silo(name string) string {
	return `apiVersion: kevaso/v1
kind: Silo
metadata: { name: ` + name + `, namespace: ` + h.ns + ` }
spec:
  runtime: docker
  image: docker.io/library/nginx:1.27-alpine
  resources: { memory: 64Mi }
`
}

type siloView struct {
	Status struct {
		Phase string `json:"phase"`
		Host  string `json:"host"`
	} `json:"status"`
}

// running waits for the silo to run, and says where.
func (h *ha) running(name string, d time.Duration) string {
	h.t.Helper()
	var s siloView
	eventually(h.t, name+" running", d, func() (bool, string) {
		out, err := h.c.kvo("", "get", "silo", name, "-n", h.ns, "-o", "json")
		if err != nil {
			return false, out
		}
		s = siloView{}
		_ = json.Unmarshal([]byte(out), &s)
		return s.Status.Phase == "Running" && s.Status.Host != "", fmt.Sprintf("%+v", s.Status)
	})
	return s.Status.Host
}

// containers are the silo's containers on m (not its pause container): "ID STARTED RUNNING".
func (h *ha) containers(ctx context.Context, m machine, name string) []string {
	h.t.Helper()
	out, err := h.b.ssh(ctx, m, "for c in $(docker ps -aq --filter label=kevaso.com/name="+name+" --filter label=kevaso.com/namespace="+h.ns+"); do "+
		"docker inspect -f '{{.Id}} {{.State.StartedAt}} {{.State.Running}}' $c; done")
	if err != nil {
		h.t.Fatalf("docker on %s: %v %s", m.Label, err, out)
	}
	var cs []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l != "" {
			cs = append(cs, l)
		}
	}
	return cs
}

// one says the silo has exactly one container on m, running, and returns it.
func (h *ha) one(ctx context.Context, m machine, name string) string {
	h.t.Helper()
	cs := h.containers(ctx, m, name)
	if len(cs) != 1 || !strings.HasSuffix(cs[0], " true") {
		h.t.Fatalf("silo %s on %s: want one running container, have %q", name, m.Label, cs)
	}
	return cs[0]
}

func (h *ha) hosts() map[string]hostView {
	h.t.Helper()
	hs, err := h.c.hosts()
	if err != nil {
		h.t.Fatalf("kvo get hosts: %v", err)
	}
	return hs
}

// leaders are the hosts that say they hold the controllers' lease, among names.
func leaders(hs map[string]hostView, names ...string) []string {
	var out []string
	for _, n := range names {
		if hs[n].leader() {
			out = append(out, n)
		}
	}
	return out
}

// exec runs a command in the silo through kvo (at url, or the context's servers when url is "").
func (h *ha) exec(url, name string, cmd ...string) (string, error) {
	args := append([]string{"exec", "silo", name, "-n", h.ns, "--"}, cmd...)
	if url == "" {
		return h.c.kvo("", args...)
	}
	return h.c.kvoAt(url, args...)
}

// unitState is how often systemd has started kevasod, and since when (a restart changes it).
func (h *ha) unitState(ctx context.Context, m machine) string {
	out, _ := h.b.ssh(ctx, m, "systemctl show kevasod -p ActiveEnterTimestampMonotonic -p NRestarts --value | tr '\\n' ' '")
	return strings.TrimSpace(out)
}

// haThree: steps 1, 2, 3, 6 and 5 on three system nodes and a worker.
func haThree(t *testing.T, ctx context.Context, b *bed, k *kit, from, to string) {
	h := newHA(t, ctx, b, k, "ha", "cp1", "cp2", "cp3", "w1")
	c, ms, ssh := h.c, h.ms, b.sshFlags()
	cps := []string{"cp1", "cp2", "cp3"}

	// 1. One system node on etcd, a worker with docker, a container silo.
	t.Log(c.mustKvoadm(append([]string{"init", "root@" + ms["cp1"].Public, "--kevasod", from, "--name", "cp1", "--cluster", "ha",
		"--store", "etcd", "--api-server", "https://" + ms["cp1"].Public + ":6443"}, ssh...)...))
	if cp := h.hosts()["cp1"].Status.ControlPlane; cp == nil || cp.Store != "etcd" || cp.PeerURL == "" {
		t.Fatalf("cp1's store: %+v", cp)
	}
	t.Log(c.mustKvoadm(append([]string{"host", "add", "root@" + ms["w1"].Public, "--kevasod", from, "--name", "w1",
		"--install", "worker", "--runtime", "docker", "--region", b.region}, ssh...)...))
	c.mustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: ha }\n---\n"+h.silo("a"), "apply", "-f", "-")
	if on := h.running("a", 8*time.Minute); on != "w1" {
		t.Fatalf("a on %s", on)
	}
	aBefore := h.one(ctx, ms["w1"], "a")
	t.Logf("step 1: cp1 on etcd, w1 with docker, silo a running (%s)", aBefore)

	// 2. Two more system nodes join: all three vote, each serves the API, kvo lists all three.
	for _, n := range []string{"cp2", "cp3"} {
		t.Log(c.mustKvoadm(append([]string{"host", "add", "root@" + ms[n].Public, "--kevasod", from, "--name", n, "--install", "control-plane"}, ssh...)...))
	}
	var urls []string
	eventually(t, "three voters serving the API", 3*time.Minute, func() (bool, string) {
		hs, err := c.hosts()
		if err != nil {
			return false, err.Error()
		}
		urls = nil
		var s []string
		for _, n := range cps {
			s = append(s, fmt.Sprintf("%s=%s/%s", n, hs[n].member(), hs[n].apiURL()))
			if hs[n].member() == "voter" && hs[n].apiURL() != "" && hs[n].ready() {
				urls = append(urls, hs[n].apiURL())
			}
		}
		return len(urls) == 3, strings.Join(s, " ")
	})
	for _, u := range urls {
		if !strings.Contains(c.servers(), u) {
			t.Fatalf("kvo context lists %q, not %s", c.servers(), u)
		}
	}
	for _, u := range urls {
		if out, err := c.kvoAt(u, "get", "silo", "a", "-n", "ha"); err != nil || !strings.Contains(out, "Running") {
			t.Fatalf("API at %s: %v %s", u, err, out)
		}
		eventually(t, "exec through "+u, 2*time.Minute, func() (bool, string) { // the agent holds a tunnel to each
			out, err := h.exec(u, "a", "echo", "hello-from-a")
			return err == nil && strings.Contains(out, "hello-from-a"), fmt.Sprint(err, out)
		})
	}
	t.Logf("step 2: cp1, cp2, cp3 vote; each serves the API and execs; kvo lists %s", c.servers())

	// 3. Lose one: the leader, powered off with nothing stopped first.
	var lead string
	eventually(t, "one leader", 2*time.Minute, func() (bool, string) {
		l := leaders(h.hosts(), cps...)
		if len(l) == 1 {
			lead = l[0]
		}
		return len(l) == 1, fmt.Sprint(l)
	})
	var rest []string
	for _, n := range cps {
		if n != lead {
			rest = append(rest, n)
		}
	}
	b.powerOff(ctx, ms[lead])
	lost := time.Now()
	t.Logf("step 3: %s (the leader) powered off", lead)
	took := time.Now()
	if out, err := c.kvo("", "get", "hosts"); err != nil {
		t.Fatalf("the API with %s off: %v %s", lead, err, out)
	}
	// kvo starts at the first server it lists every time: with that one off, failing over must
	// not wait for the OS's connect timeout.
	if d := time.Since(took); d > 20*time.Second {
		t.Fatalf("kvo get hosts with %s off took %v", lead, d.Round(time.Second))
	}
	var newLead string
	eventually(t, "a new leader", 3*time.Minute, func() (bool, string) {
		hs, err := c.hosts()
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
	t.Logf("step 3: %s leads after %s", newLead, time.Since(lost).Round(time.Second))
	c.mustKvo(h.silo("b"), "apply", "-f", "-")
	if on := h.running("b", 5*time.Minute); on != "w1" {
		t.Fatalf("b on %s", on)
	}
	if out, err := h.exec("", "a", "echo", "still-here"); err != nil || !strings.Contains(out, "still-here") {
		t.Fatalf("exec into a with %s off: %v %s", lead, err, out)
	}
	if got := h.one(ctx, ms["w1"], "a"); got != aBefore {
		t.Fatalf("a's container changed while %s was off: %s, was %s", lead, got, aBefore)
	}
	bOnce := h.one(ctx, ms["w1"], "b")
	t.Logf("step 3: API, scheduling (b on w1) and exec work without %s; a untouched, b started once", lead)

	// A removal that would lose the majority (one of two healthy of three) is refused.
	eventually(t, lead+" seen as down", 2*time.Minute, func() (bool, string) {
		hb := h.hosts()[lead].Status.LastHeartbeat
		return hb != nil && time.Since(*hb) > 50*time.Second, fmt.Sprint(hb)
	})
	out, err := c.kvoadm("host", "remove", rest[0])
	if err == nil || !strings.Contains(out, "majority") {
		t.Fatalf("removing %s with %s off: want a refusal about the majority, got %v\n%s", rest[0], lead, err, out)
	}
	if hs := h.hosts(); hs[rest[0]].Status.ControlPlane == nil || hs[rest[0]].member() != "voter" {
		t.Fatalf("refused removal changed %s: %+v", rest[0], hs[rest[0]].Status.ControlPlane)
	}
	t.Logf("step 5 (refusal): host remove %s refused with %s down: %s", rest[0], lead, firstLine(out))

	// Back on: it rejoins and catches up (it holds b, written while it was off).
	b.powerOn(ctx, ms[lead])
	leadURL := "https://" + ms[lead].Public + ":6443"
	eventually(t, lead+" back as a voter", 5*time.Minute, func() (bool, string) {
		hl := h.hosts()[lead]
		return hl.ready() && hl.member() == "voter", fmt.Sprintf("ready=%v member=%s", hl.ready(), hl.member())
	})
	eventually(t, lead+" serving what was written while it was off", 2*time.Minute, func() (bool, string) {
		out, err := c.kvoAt(leadURL, "get", "silo", "b", "-n", "ha")
		return err == nil && strings.Contains(out, "Running"), fmt.Sprint(err, out)
	})
	if got := h.one(ctx, ms["w1"], "b"); got != bOnce {
		t.Fatalf("b restarted after %s came back: %s, was %s", lead, got, bOnce)
	}
	if l := leaders(h.hosts(), cps...); len(l) != 1 {
		t.Fatalf("leaders after %s came back: %v", lead, l)
	}
	t.Logf("step 3: %s back, a voter again, caught up; still one leader", lead)

	// 6. Upgrade: system nodes one at a time while the API is polled, then the worker, whose
	// container never stops.
	aBefore = h.one(ctx, ms["w1"], "a")
	var mu sync.Mutex
	okN, fails := 0, []string{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			out, err := c.kvo("", "get", "hosts")
			mu.Lock()
			if err == nil {
				okN++
			} else {
				fails = append(fails, time.Now().Format(time.TimeOnly)+" "+firstLine(out))
			}
			mu.Unlock()
		}
	}()
	upOut, upErr := c.kvoadm("upgrade", "--kevasod", to, "--ssh-key", b.key)
	close(stop)
	wg.Wait()
	t.Log(upOut)
	if upErr != nil {
		t.Fatalf("kvoadm upgrade: %v", upErr)
	}
	t.Logf("step 6: API polled %d times during the roll, %d failures", okN+len(fails), len(fails))
	if len(fails) > 0 {
		t.Errorf("the API failed during the upgrade: %s", strings.Join(fails, "; "))
	}
	hs := h.hosts()
	for _, n := range append(cps, "w1") {
		if hs[n].Status.Version != "v0.16.1" || !hs[n].ready() {
			t.Fatalf("%s after the upgrade: version %s ready %v", n, hs[n].Status.Version, hs[n].ready())
		}
	}
	if got := h.one(ctx, ms["w1"], "a"); got != aBefore {
		t.Fatalf("a's container restarted in the upgrade: %s, was %s", got, aBefore)
	}
	t.Logf("step 6: every host at v0.16.1; a kept its container (%s)", aBefore)

	// 5. Remove a system node cleanly: its member goes first, then the Host.
	victim := ""
	for _, n := range cps {
		if !hs[n].leader() {
			victim = n
		}
	}
	out = c.mustKvoadm("host", "remove", victim)
	t.Log(out)
	i, j := strings.Index(out, "left the store"), strings.Index(out, "removed from the cluster")
	if i < 0 || j < i {
		t.Fatalf("host remove %s: want the member removed before the host:\n%s", victim, out)
	}
	hs = h.hosts()
	if _, ok := hs[victim]; ok {
		t.Fatalf("%s still a Host", victim)
	}
	voters := 0
	for _, n := range cps {
		if hs[n].member() == "voter" {
			voters++
		}
	}
	if voters != 2 {
		t.Fatalf("voters after removing %s: %d", victim, voters)
	}
	c.mustKvo(h.silo("c"), "apply", "-f", "-")
	h.running("c", 5*time.Minute)
	t.Logf("step 5: %s removed (member first, then the Host); two voters left and the cluster still schedules", victim)
	h.done(ctx)
}

// objects is every object of the kinds the migration must keep: "KIND NS/NAME UID".
func (h *ha) objects() []string {
	h.t.Helper()
	var out []string
	for _, k := range []struct {
		kind string
		ns   bool
	}{{"namespaces", false}, {"hosts", false}, {"clusterroles", false}, {"clusterrolebindings", false}, {"ippools", false},
		{"roles", true}, {"rolebindings", true}, {"serviceaccounts", true}, {"secrets", true}, {"silos", true}} {
		args := []string{"get", k.kind, "-o", "json"}
		if k.ns {
			args = append(args, "-A")
		}
		raw := h.c.mustKvo("", args...)
		if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "null" || strings.TrimSpace(raw) == "[]" {
			continue
		}
		l, err := decodeList[struct {
			Metadata struct{ Name, Namespace, UID string } `json:"metadata"`
		}](raw)
		if err != nil {
			h.t.Fatalf("kvo get %s: %v\n%s", k.kind, err, raw)
		}
		for _, o := range l {
			out = append(out, fmt.Sprintf("%s %s/%s %s", k.kind, o.Metadata.Namespace, o.Metadata.Name, o.Metadata.UID))
		}
	}
	sort.Strings(out)
	return out
}

// haLight: step 7 (a cluster on SQLite moved to etcd), then step 4 (two system nodes and a light
// one; losing a full one).
func haLight(t *testing.T, ctx context.Context, b *bed, k *kit, kevasod string) {
	h := newHA(t, ctx, b, k, "mig", "m1", "m2", "light", "mw")
	c, ms, ssh := h.c, h.ms, b.sshFlags()

	// 7. Started on SQLite, with objects and a worker, then switched to etcd.
	t.Log(c.mustKvoadm(append([]string{"init", "root@" + ms["m1"].Public, "--kevasod", kevasod, "--name", "m1", "--cluster", "mig",
		"--api-server", "https://" + ms["m1"].Public + ":6443"}, ssh...)...))
	if cp := h.hosts()["m1"].Status.ControlPlane; cp == nil || cp.Store != "sqlite" {
		t.Fatalf("m1's store: %+v", cp)
	}
	t.Log(c.mustKvoadm(append([]string{"host", "add", "root@" + ms["mw"].Public, "--kevasod", kevasod, "--name", "mw",
		"--install", "worker", "--runtime", "docker", "--region", b.region}, ssh...)...))
	c.mustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: mig }\n---\n"+h.silo("keep")+"---\n"+h.silo("gone"), "apply", "-f", "-")
	c.mustKvo("a value kept across the move", "create", "secret", "s1", "-n", "mig", "--from-file", "-")
	h.running("keep", 8*time.Minute)
	h.running("gone", 3*time.Minute)
	if out, err := c.kvoadm(append([]string{"host", "add", "root@" + ms["m2"].Public, "--kevasod", kevasod, "--name", "m2", "--install", "control-plane"}, ssh...)...); err == nil || !strings.Contains(out, "kvoadm store migrate") {
		t.Fatalf("host add --install control-plane on SQLite: want a refusal naming store migrate, got %v\n%s", err, out)
	}
	before := h.objects()
	keep := h.one(ctx, ms["mw"], "keep")
	agent := h.unitState(ctx, ms["mw"])
	t.Logf("step 7: m1 on SQLite with %d objects; host add of a system node refused until migrated", len(before))

	t.Log(c.mustKvoadm(append([]string{"store", "migrate", "root@" + ms["m1"].Public}, ssh...)...))
	if cp := h.hosts()["m1"].Status.ControlPlane; cp == nil || cp.Store != "etcd" || cp.PeerURL == "" {
		t.Fatalf("m1's store after migrate: %+v", cp)
	}
	if out, err := b.ssh(ctx, ms["m1"], "ls /var/lib/kevaso/store.db.migrated && ! ls /var/lib/kevaso/store.db"); err != nil {
		t.Fatalf("store.db after the move: %v %s", err, out)
	}
	after := h.objects()
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("objects changed in the move:\nbefore:\n%s\nafter:\n%s", strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	if out := c.mustKvo("", "get", "secret", "s1", "-n", "mig", "--reveal"); !strings.Contains(out, "a value kept across the move") {
		t.Fatalf("secret after the move: %q", out)
	}
	// The worker's agent wasn't restarted: it re-lists by itself. A new silo starts, a deleted one goes.
	c.mustKvo(h.silo("fresh"), "apply", "-f", "-")
	h.running("fresh", 5*time.Minute)
	c.mustKvo("", "delete", "silo", "gone", "-n", "mig")
	eventually(t, "gone's container removed from mw", 3*time.Minute, func() (bool, string) {
		cs := h.containers(ctx, ms["mw"], "gone")
		return len(cs) == 0, fmt.Sprint(cs)
	})
	if got := h.one(ctx, ms["mw"], "keep"); got != keep {
		t.Fatalf("keep's container changed in the move: %s, was %s", got, keep)
	}
	if got := h.unitState(ctx, ms["mw"]); got != agent {
		t.Fatalf("mw's kevasod restarted (%s, was %s): its watches should re-list on their own", got, agent)
	}
	t.Logf("step 7: all %d objects kept with their UIDs, the secret opens, mw's agent re-listed by itself (fresh started, gone removed)", len(after))

	// 4. Two system nodes and a light one.
	t.Log(c.mustKvoadm(append([]string{"host", "add", "root@" + ms["m2"].Public, "--kevasod", kevasod, "--name", "m2", "--install", "control-plane"}, ssh...)...))
	t.Log(c.mustKvoadm(append([]string{"host", "add", "root@" + ms["light"].Public, "--kevasod", kevasod, "--name", "light", "--install", "control-plane", "--state-only"}, ssh...)...))
	eventually(t, "three voters", 3*time.Minute, func() (bool, string) {
		hs, err := c.hosts()
		if err != nil {
			return false, err.Error()
		}
		n := 0
		for _, x := range []string{"m1", "m2", "light"} {
			if hs[x].member() == "voter" && hs[x].ready() {
				n++
			}
		}
		return n == 3, fmt.Sprint(n)
	})
	hs := h.hosts()
	if hs["light"].apiURL() != "" || hs["light"].leader() {
		t.Fatalf("the light node: %+v", hs["light"].Status.ControlPlane)
	}
	if conn, err := net.DialTimeout("tcp", ms["light"].Public+":6443", 5*time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the light node listens on 6443")
	}
	if strings.Contains(c.servers(), ms["light"].Public) {
		t.Fatalf("kvo lists the light node: %s", c.servers())
	}
	var lead string
	eventually(t, "one leader of m1, m2", 2*time.Minute, func() (bool, string) {
		hs := h.hosts()
		l := leaders(hs, "m1", "m2", "light")
		if len(l) == 1 && l[0] != "light" {
			lead = l[0]
		}
		return lead != "", fmt.Sprint(l)
	})
	other := map[string]string{"m1": "m2", "m2": "m1"}[lead]
	t.Logf("step 4: m1, m2 and light vote; light serves no API (nothing on 6443) and doesn't lead; %s leads", lead)

	b.powerOff(ctx, ms[lead])
	t.Logf("step 4: %s powered off", lead)
	eventually(t, other+" leads", 3*time.Minute, func() (bool, string) {
		hs, err := c.hosts()
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, other, "light")
		return len(l) == 1 && l[0] == other, fmt.Sprint(l)
	})
	c.mustKvo(h.silo("after"), "apply", "-f", "-")
	if on := h.running("after", 5*time.Minute); on != "mw" {
		t.Fatalf("after on %s", on)
	}
	if out, err := h.exec("", "keep", "echo", "hi"); err != nil || !strings.Contains(out, "hi") {
		t.Fatalf("exec into keep with %s off: %v %s", lead, err, out)
	}
	if hs := h.hosts(); hs["light"].leader() || hs["light"].apiURL() != "" {
		t.Fatalf("the light node took over: %+v", hs["light"].Status.ControlPlane)
	}
	t.Logf("step 4: with %s off, %s leads, writes go through (light's vote), a silo is scheduled, exec works; light still only votes", lead, other)
	h.done(ctx)
}

// TestSweep deletes every Linode labelled or tagged with KEVASO_E2E_SWEEP (a family prefix such
// as kvt-ha-): what a run that died left behind. Nothing else is touched.
//
//	KEVASO_E2E_SWEEP=kvt-ha- KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -run TestSweep ./test/e2e/
func TestSweep(t *testing.T) {
	prefix, tokenFile := os.Getenv("KEVASO_E2E_SWEEP"), os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if prefix == "" || tokenFile == "" {
		t.Skip("KEVASO_E2E_SWEEP and KEVASO_LINODE_TOKEN_FILE not set")
	}
	if !strings.HasSuffix(prefix, "-") || len(prefix) < len("kvt-x-") {
		t.Fatalf("prefix %q: want a family prefix ending in -, like kvt-ha-", prefix)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, prefix)
	for _, in := range instancesOf(ctx, b.api, prefix) {
		t.Logf("deleting Linode %s (%d)", in.Label, in.ID)
		_ = b.api.DeleteInstance(ctx, in.ID)
	}
	eventually(t, "nothing labelled "+prefix, 5*time.Minute, func() (bool, string) {
		var left []string
		for _, in := range instancesOf(ctx, b.api, prefix) {
			left = append(left, in.Label)
		}
		return len(left) == 0, strings.Join(left, ", ")
	})
	t.Logf("no Linode labelled %s… is left", prefix)
}
