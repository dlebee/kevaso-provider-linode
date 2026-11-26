//go:build e2e

// TestHA is the part of M16's "done when" that TestHA3 (three system nodes, the rules of
// 2026-11-14) doesn't cover, on real Linodes: a cluster started on SQLite and moved to etcd with
// kvoadm store migrate (step 7), then two system nodes and a light, state-only one, and losing a
// full one (step 4). Linodes have no KVM, so the silos are containers (docker).
//
//	KEVASO_LINODE_TOKEN_FILE=~/linode-token.txt go test -tags e2e -v -run 'TestHA$' -timeout 90m ./test/e2e/
//
// Four Linodes. Everything is labelled and tagged kvt-ha-RUN and swept at the end.
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
	"testing"
	"time"
)

func TestHA(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 85*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-ha-")
	t.Logf("run %s, region %s", b.Tag, b.Region)
	b.CleanupAtEnd(t)
	k := newKit(t, ctx)
	haLight(t, ctx, b, k, k.Build("kevasod-v0.16.0", "kevaso.com/core/cmd/kevasod", "linux", "v0.16.0"))
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
	h := &ha{t: t, b: b, c: k.Cluster(), ms: b.CreateAll(ctx, roles...), ns: ns}
	t.Cleanup(func() { // before the sweep: what was going on, if it failed
		if !t.Failed() {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		for _, r := range roles {
			out, _ := b.SSH(dctx, h.ms[r], "systemctl is-active kevasod; grep ExecStart /etc/systemd/system/kevasod.service; "+
				"journalctl -u kevasod -n 120 --no-pager 2>&1 | grep -iv heartbeat | tail -80")
			t.Logf("── %s ──\n%s", r, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "yaml"}, {"get", "silos", "-A", "-o", "wide"}} {
			c := exec.CommandContext(dctx, k.Kvo, args...)
			c.Env = h.c.Env
			out, _ := c.CombinedOutput()
			t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})
	return h
}

// done deletes the part's machines and waits until the account no longer lists them.
func (h *ha) done(ctx context.Context) {
	for _, m := range h.ms {
		h.b.Destroy(ctx, m)
	}
	eventually(h.t, "the part's Linodes gone", 5*time.Minute, func() (bool, string) {
		var left []string
		for _, in := range h.b.Instances(ctx) {
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
  container: { image: docker.io/library/nginx:1.27-alpine }
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
		out, err := h.c.Kvo("", "get", "silo", name, "-n", h.ns, "-o", "json")
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
	out, err := h.b.SSH(ctx, m, "for c in $(docker ps -aq --filter label=kevaso.com/name="+name+" --filter label=kevaso.com/namespace="+h.ns+"); do "+
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

// hosts lists the hosts, asking up to three times: with one system node, a busy one answers a
// read after kvo's 6 s deadline now and then (core b10eb76), and there is no other to fail over to.
func (h *ha) hosts() map[string]hostView {
	h.t.Helper()
	var err error
	for i := 0; i < 3; i++ {
		var hs map[string]hostView
		if hs, err = h.c.Hosts(); err == nil {
			return hs
		}
		h.t.Logf("kvo get hosts (try %d): %v", i+1, err)
		time.Sleep(5 * time.Second)
	}
	h.t.Fatalf("kvo get hosts: %v", err)
	return nil
}

// leaders are the hosts that say they hold the controllers' lease, among names.
func leaders(hs map[string]hostView, names ...string) []string {
	var out []string
	for _, n := range names {
		if hs[n].Leader() {
			out = append(out, n)
		}
	}
	return out
}

// exec runs a command in the silo through kvo (at url, or the context's servers when url is "").
func (h *ha) exec(url, name string, cmd ...string) (string, error) {
	args := append([]string{"exec", "silo", name, "-n", h.ns, "--"}, cmd...)
	if url == "" {
		return h.c.Kvo("", args...)
	}
	return h.c.KvoAt(url, args...)
}

// unitState is how often systemd has started kevasod, and since when (a restart changes it).
func (h *ha) unitState(ctx context.Context, m machine) string {
	out, _ := h.b.SSH(ctx, m, "systemctl show kevasod -p ActiveEnterTimestampMonotonic -p NRestarts --value | tr '\\n' ' '")
	return strings.TrimSpace(out)
}

// objects is every object of the kinds the migration must keep: "KIND NS/NAME UID".
func (h *ha) objects() []string {
	h.t.Helper()
	var out []string
	for _, k := range []struct {
		kind string
		ns   bool
	}{{"namespaces", false}, {"hosts", false}, {"clusterroles", false}, {"clusterrolebindings", false},
		{"roles", true}, {"rolebindings", true}, {"serviceaccounts", true}, {"secrets", true}, {"silos", true}} {
		args := []string{"get", k.kind, "-o", "json"}
		if k.ns {
			args = append(args, "-A")
		}
		raw := h.c.MustKvo("", args...)
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
	c, ms, ssh := h.c, h.ms, b.SSHFlags()

	// 7. Started on SQLite, with objects and a worker, then switched to etcd.
	t.Log(redact(c.MustKvoadm(append([]string{"init", "root@" + ms["m1"].Public, "--external-ip", ms["m1"].Public, "--kevasod", kevasod, "--name", "m1", "--cluster", "mig",
		"--api-server", "https://" + ms["m1"].Public + ":6443"}, ssh...)...)))
	if cp := h.hosts()["m1"].Status.ControlPlane; cp == nil || cp.Store != "sqlite" {
		t.Fatalf("m1's store: %+v", cp)
	}
	t.Log(c.MustKvoadm(append([]string{"host", "add", "root@" + ms["mw"].Public, "--external-ip", ms["mw"].Public, "--kevasod", kevasod, "--name", "mw",
		"--install", "worker", "--runtime", "docker", "--region", b.Region}, ssh...)...))
	c.MustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: mig }\n---\n"+h.silo("keep")+"---\n"+h.silo("gone"), "apply", "-f", "-")
	c.MustKvo("a value kept across the move", "create", "secret", "s1", "-n", "mig", "--from-file", "token=-")
	h.running("keep", 8*time.Minute)
	h.running("gone", 3*time.Minute)
	if out, err := c.Kvoadm(append([]string{"host", "add", "root@" + ms["m2"].Public, "--external-ip", ms["m2"].Public, "--kevasod", kevasod, "--name", "m2", "--install", "control-plane"}, ssh...)...); err == nil || !strings.Contains(out, "kvoadm store migrate") {
		t.Fatalf("host add --install control-plane on SQLite: want a refusal naming store migrate, got %v\n%s", err, out)
	}
	before := h.objects()
	keep := h.one(ctx, ms["mw"], "keep")
	agent := h.unitState(ctx, ms["mw"])
	t.Logf("step 7: m1 on SQLite with %d objects; host add of a system node refused until migrated", len(before))

	t.Log(c.MustKvoadm(append([]string{"store", "migrate", "root@" + ms["m1"].Public}, ssh...)...))
	if cp := h.hosts()["m1"].Status.ControlPlane; cp == nil || cp.Store != "etcd" || cp.PeerURL == "" {
		t.Fatalf("m1's store after migrate: %+v", cp)
	}
	if out, err := b.SSH(ctx, ms["m1"], "ls /var/lib/kevaso/store.db.migrated && ! ls /var/lib/kevaso/store.db"); err != nil {
		t.Fatalf("store.db after the move: %v %s", err, out)
	}
	after := h.objects()
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("objects changed in the move:\nbefore:\n%s\nafter:\n%s", strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	if out := c.MustKvo("", "get", "secret", "s1", "-n", "mig", "--reveal", "--key", "token"); !strings.Contains(out, "a value kept across the move") {
		t.Fatalf("secret after the move: %q", out)
	}
	// The worker's agent wasn't restarted: it re-lists by itself. A new silo starts, a deleted one goes.
	c.MustKvo(h.silo("fresh"), "apply", "-f", "-")
	h.running("fresh", 5*time.Minute)
	c.MustKvo("", "delete", "silo", "gone", "-n", "mig")
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
	t.Log(c.MustKvoadm(append([]string{"host", "add", "root@" + ms["m2"].Public, "--external-ip", ms["m2"].Public, "--kevasod", kevasod, "--name", "m2", "--install", "control-plane"}, ssh...)...))
	t.Log(c.MustKvoadm(append([]string{"host", "add", "root@" + ms["light"].Public, "--external-ip", ms["light"].Public, "--kevasod", kevasod, "--name", "light", "--install", "control-plane", "--state-only"}, ssh...)...))
	eventually(t, "three voters", 3*time.Minute, func() (bool, string) {
		hs, err := c.Hosts()
		if err != nil {
			return false, err.Error()
		}
		n := 0
		for _, x := range []string{"m1", "m2", "light"} {
			if hs[x].Member() == "voter" && hs[x].Ready() {
				n++
			}
		}
		return n == 3, fmt.Sprint(n)
	})
	hs := h.hosts()
	if hs["light"].APIURL() != "" || hs["light"].Leader() {
		t.Fatalf("the light node: %+v", hs["light"].Status.ControlPlane)
	}
	if conn, err := net.DialTimeout("tcp", ms["light"].Public+":6443", 5*time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the light node listens on 6443")
	}
	if strings.Contains(c.Servers(), ms["light"].Public) {
		t.Fatalf("kvo lists the light node: %s", c.Servers())
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

	b.PowerOff(ctx, ms[lead])
	t.Logf("step 4: %s powered off", lead)
	eventually(t, other+" leads", 3*time.Minute, func() (bool, string) {
		hs, err := c.Hosts()
		if err != nil {
			return false, err.Error()
		}
		l := leaders(hs, other, "light")
		return len(l) == 1 && l[0] == other, fmt.Sprint(l)
	})
	c.MustKvo(h.silo("after"), "apply", "-f", "-")
	if on := h.running("after", 5*time.Minute); on != "mw" {
		t.Fatalf("after on %s", on)
	}
	if out, err := h.exec("", "keep", "echo", "hi"); err != nil || !strings.Contains(out, "hi") {
		t.Fatalf("exec into keep with %s off: %v %s", lead, err, out)
	}
	if hs := h.hosts(); hs["light"].Leader() || hs["light"].APIURL() != "" {
		t.Fatalf("the light node took over: %+v", hs["light"].Status.ControlPlane)
	}
	t.Logf("step 4: with %s off, %s leads, writes go through (light's vote), a silo is scheduled, exec works; light still only votes", lead, other)
	h.done(ctx)
}
