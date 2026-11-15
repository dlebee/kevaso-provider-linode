//go:build e2e

// TestProvider is the provider's own features under core's sealed model, on real Linodes, as far as
// the token's scopes go: a control plane and two docker workers, the plugin installed with kvoadm,
// then
//
//   - addresses: each worker's externalIP as declared, its internal IP the Linode's private
//     address (README, "Addresses"); VLAN and VPC addresses aren't read (README);
//   - block storage (linode-block): two volumes of one silo are two Linode volumes, each with its
//     identity tag; attached to the silo's host; moved with the silo (kvo move), data kept, the
//     same Linode volumes; one removed from the spec is deleted, the other kept; a label already
//     taken by a volume Kevaso didn't make is refused, never adopted (K-035); everything deleted with
//     the silo, and only then; a volume name too long for a label refused by admission;
//   - Kevaso's own rules, checked from this Mac: a port opened with hostPort: [external] answers;
//     every other TCP listener on every machine (but SSH, and the API on the control plane) is
//     closed from here; [internal] answers on the private network only.
//
// Skipped, with the reason: NodeBalancers and DNS (the token has no NodeBalancers or Domains
// scope: TestLinode covers them when it does), and a Linode Cloud Firewall (the provider makes
// none, and the token has no Firewalls scope).
//
//	KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -v -run 'TestProvider$' -timeout 90m ./test/e2e/
package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/provider-linode/linode"
)

func TestProvider(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 85*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-pf-")
	t.Logf("run %s, region %s", b.Tag, b.Region)
	b.CleanupAtEnd(t)
	k := newKit(t, ctx)
	home := t.TempDir()
	cfg := filepath.Join(home, ".kevaso", "config.yaml")
	h := &ha3{t: t, ctx: ctx, b: b, k: k, built: map[string]string{},
		c: &cluster{K: k, Home: home, Cfg: cfg, Env: append(os.Environ(), "HOME="+home, "KVO_CONFIG="+cfg, "KVO_SERVER=", "KVO_TOKEN=")}}
	h.diagnoseOnFailure()
	h.ms = b.CreateAll(ctx, "cp", "w1", "w2")
	h.ssh = b.SSHFlags()
	kevasod := h.kevasod("")
	plugin := k.Build("kevaso-provider-linode", "kevaso.com/provider-linode/cmd/kevaso-provider-linode", "linux", "")
	cp := h.ms["cp"]
	t.Log(redact(h.mustKvoadm("", append([]string{"init", "root@" + cp.Public, "--external-ip", cp.Public, "--kevasod", kevasod, "--name", "cp", "--cluster", "pf",
		"--api-server", "https://" + cp.Public + ":6443"}, h.ssh...)...)))
	for _, w := range []string{"w1", "w2"} {
		m := h.ms[w]
		t.Log(h.mustKvoadm("", append([]string{"host", "add", "root@" + m.Public, "--external-ip", m.Public, "--kevasod", kevasod, "--name", w,
			"--install", "worker", "--runtime", "docker", "--region", b.Region, "--label", "kvt/host=" + w}, h.ssh...)...))
	}
	labelPrefix := b.Tag + "-"
	cfgFile := filepath.Join(t.TempDir(), "linode.json")
	_ = os.WriteFile(cfgFile, []byte(fmt.Sprintf(`{"tags": [%q], "labelPrefix": %q}`, b.Tag, labelPrefix)), 0o644)
	t.Log(h.mustKvoadm("", append([]string{"plugin", "install", "root@" + cp.Public, "--name", "linode", "--binary", plugin, "--manifest", manifestFile(t),
		"--credential", "token=" + tokenFile, "--config", cfgFile}, h.ssh...)...))
	nodeCfg := filepath.Join(t.TempDir(), "node.json")
	_ = os.WriteFile(nodeCfg, []byte(fmt.Sprintf(`{"region": %q}`, b.Region)), 0o644)
	for _, w := range []string{"w1", "w2"} {
		t.Log(h.mustKvoadm("", append([]string{"plugin", "install", "root@" + h.ms[w].Public, "--name", "linode", "--binary", plugin, "--manifest", manifestFile(t),
			"--side", "node", "--config", nodeCfg}, h.ssh...)...))
	}

	t.Run("addresses", func(t *testing.T) { pfAddresses(t, h) })
	t.Run("volumes", func(t *testing.T) { pfVolumes(t, h, labelPrefix) })
	t.Run("rules", func(t *testing.T) { pfRules(t, h) })
	t.Run("nodebalancer", func(t *testing.T) {
		if _, err := b.API.ListNodeBalancers(ctx, nil); err != nil {
			t.Skipf("the token can't use NodeBalancers (%v): TestLinode tests linode-nodebalancer with a token that can", err)
		}
		t.Skip("NodeBalancers are TestLinode's")
	})
	t.Run("dns", func(t *testing.T) {
		if _, err := b.API.ListDomains(ctx, nil); err != nil {
			t.Skipf("the token can't use Domains (%v): TestLinode tests the linode DNS provider with a token that can", err)
		}
		t.Skip("DNS is TestLinode's")
	})
	t.Run("cloud-firewall", func(t *testing.T) {
		_, err := b.API.ListFirewalls(ctx, nil)
		t.Skipf("the provider makes no Linode Cloud Firewall (Kevaso's rules are nftables on each host, checked in rules); the token's Firewalls scope: %v", err)
	})
}

// pfAddresses: what the README says the provider makes of a Linode's addresses.
func pfAddresses(t *testing.T, h *ha3) {
	eventually(t, "the workers recognised as Linodes", 3*time.Minute, func() (bool, string) {
		hs, err := h.hostsWithin(30 * time.Second)
		if err != nil {
			return false, err.Error()
		}
		return hs["w1"].Ready() && hs["w2"].Ready(), "not yet"
	})
	list := h.mustKvoadm("", "host", "list")
	t.Logf("kvoadm host list:\n%s", list)
	type hostAddrs struct {
		Spec struct {
			InternalIP string `json:"internalIP"`
			ExternalIP string `json:"externalIP"`
		} `json:"spec"`
		Status struct {
			Provider *struct{ Name, ID, Region, PrivateAddress string } `json:"provider"`
		} `json:"status"`
	}
	for _, w := range []string{"w1", "w2"} {
		var hv hostAddrs
		eventually(t, w+"'s provider status", 3*time.Minute, func() (bool, string) {
			out, err := h.c.Kvo("", "get", "host", w, "-o", "json")
			if err != nil {
				return false, out
			}
			l, err := decodeList[hostAddrs](out)
			if err != nil || len(l) != 1 {
				return false, fmt.Sprint(err)
			}
			hv = l[0]
			return hv.Status.Provider != nil && hv.Status.Provider.Name == "linode", out
		})
		m := h.ms[w]
		p := hv.Status.Provider
		if hv.Spec.ExternalIP != m.Public || hv.Spec.InternalIP != "" {
			t.Errorf("%s: spec external %q internal %q: want the declared %s and no internal IP (the provider sets neither)", w, hv.Spec.ExternalIP, hv.Spec.InternalIP, m.Public)
		}
		if p.ID != strconv.Itoa(m.ID) || p.Region != h.b.Region || p.PrivateAddress != m.Private {
			t.Errorf("%s: provider status %+v, want Linode %d in %s, private %s", w, *p, m.ID, h.b.Region, m.Private)
		}
		var line string
		for _, l := range strings.Split(list, "\n") {
			if f := strings.Fields(l); len(f) > 2 && f[0] == w {
				line = l
			}
		}
		if f := strings.Fields(line); len(f) < 3 || f[1] != m.Private || f[2] != m.Public {
			t.Errorf("%s in kvoadm host list: %q, want internal %s (the private address) and external %s", w, line, m.Private, m.Public)
		}
		t.Logf("%s: external %s as declared; internal %s, the Linode's private address (status.provider); Linode %s in %s", w, m.Public, p.PrivateAddress, p.ID, p.Region)
	}
}

// volView is the part of a silo the volume steps read.
type volView struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Phase   string `json:"phase"`
		Host    string `json:"host"`
		Volumes []struct {
			Name, Handle, Device, Message string
			Mounted                       bool
		} `json:"volumes"`
		Conditions []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func (h *ha3) volSilo(ns, name string) (volView, string) {
	var s volView
	out, err := h.c.Kvo("", "get", "silo", name, "-n", ns, "-o", "json")
	if err != nil {
		return s, out
	}
	if l, err := decodeList[volView](out); err == nil && len(l) == 1 {
		s = l[0]
	}
	return s, out
}

// runVolumes are the run's Linode volumes, by label.
func (h *ha3) runVolumes(t *testing.T) map[string]linodego.Volume {
	t.Helper()
	_, vols, _, _, err := h.b.Owned(h.ctx, h.b.RunOwns)
	if err != nil {
		t.Fatalf("list volumes: %v", err)
	}
	m := map[string]linodego.Volume{}
	for _, v := range vols {
		m[v.Label] = v
	}
	return m
}

func dbYAML(ns string, vols ...string) string {
	s := `apiVersion: kevaso/v1
kind: Silo
metadata: { name: db, namespace: ` + ns + ` }
spec:
  runtime: docker
  container: { image: docker.io/library/nginx:1.27-alpine }
  resources: { memory: 64Mi }
  volumes:
`
	for _, v := range vols {
		s += "    - { name: " + v + ", class: linode-block, size: 10Gi, mountPath: /" + v + " }\n"
	}
	return s
}

func pfVolumes(t *testing.T, h *ha3, labelPrefix string) {
	const ns = "pf"
	h.namespace(t, ns)
	ctx := h.ctx

	// Admission refuses a volume whose label would be cut (core's half of K-035).
	long := strings.Replace(dbYAML(ns, "data"), "name: db,", "name: a-silo-name-this-long,", 1)
	if out, err := h.c.Kvo(long, "apply", "-f", "-"); err == nil || !strings.Contains(out, "shorten") {
		t.Errorf("a volume label too long: %v %s", err, out)
	} else {
		t.Logf("too long a label, refused: %s", firstLine(out))
	}

	// Two volumes, two Linode volumes, each with its identity, attached to the silo's host.
	h.apply(t, dbYAML(ns, "data", "wal"))
	var s volView
	eventually(t, "db running with both volumes mounted", 10*time.Minute, func() (bool, string) {
		var out string
		s, out = h.volSilo(ns, "db")
		ok := s.Status.Phase == "Running" && len(s.Status.Volumes) == 2
		for _, v := range s.Status.Volumes {
			ok = ok && v.Mounted
		}
		return ok, firstLine(out) + fmt.Sprintf(" %+v", s.Status)
	})
	vols := h.runVolumes(t)
	if len(vols) != 2 {
		t.Fatalf("db's two volumes are %d Linode volumes: %v", len(vols), vols)
	}
	handles := map[string]string{} // volume name → handle
	for _, v := range s.Status.Volumes {
		handles[v.Name] = v.Handle
	}
	if handles["data"] == handles["wal"] {
		t.Fatalf("data and wal share a handle: %v", handles)
	}
	host := h.ms[s.Status.Host]
	for _, v := range vols {
		var tag string
		for _, tg := range v.Tags {
			if strings.HasPrefix(tg, "kvo-vol-") {
				tag = tg
			}
		}
		if tag == "" || v.LinodeID == nil || *v.LinodeID != host.ID || !strings.HasPrefix(v.Label, labelPrefix) {
			t.Errorf("Linode volume %s: tags %v, attached to %v (want %d, %s)", v.Label, v.Tags, v.LinodeID, host.ID, s.Status.Host)
		}
		if !slices.ContainsFunc([]string{handles["data"], handles["wal"]}, func(hd string) bool { return strings.HasPrefix(hd, strconv.Itoa(v.ID)+":") }) {
			t.Errorf("Linode volume %d (%s) isn't one of db's handles %v", v.ID, v.Label, handles)
		}
	}
	t.Logf("db on %s: two Linode volumes, %v, handles %v", s.Status.Host, keys(vols), handles)
	h.c.MustKvo("", "exec", "silo", "db", "-n", ns, "--", "sh", "-c", "echo d > /data/m && echo w > /wal/m && sync")

	// Moved with the silo: the same Linode volumes, attached to the other host, data kept.
	other := map[string]string{"w1": "w2", "w2": "w1"}[s.Status.Host]
	start := time.Now()
	h.c.MustKvo("", "move", "silo", "db", "-n", ns, "--to", other)
	eventually(t, "db running on "+other+" with its volumes", 10*time.Minute, func() (bool, string) {
		s, _ = h.volSilo(ns, "db")
		ok := s.Status.Phase == "Running" && s.Status.Host == other && len(s.Status.Volumes) == 2
		for _, v := range s.Status.Volumes {
			ok = ok && v.Mounted
		}
		return ok, fmt.Sprintf("%+v", s.Status)
	})
	t.Logf("db moved to %s in %s", other, time.Since(start).Round(time.Second))
	moved := h.runVolumes(t)
	for l, v := range vols {
		w, ok := moved[l]
		if !ok || w.ID != v.ID || w.LinodeID == nil || *w.LinodeID != h.ms[other].ID {
			t.Errorf("after the move, Linode volume %s: %+v (want the same volume %d, attached to %s)", l, w, v.ID, other)
		}
	}
	if got := h.c.MustKvo("", "exec", "silo", "db", "-n", ns, "--", "cat", "/data/m", "/wal/m"); strings.Fields(got)[0] != "d" || !strings.Contains(got, "w") {
		t.Errorf("data after the move: %q", got)
	}
	t.Log("moved: the same two Linode volumes, now on " + other + ", with their data")

	// A label taken by a volume Kevaso didn't make: refused, never adopted.
	p := &linode.Provider{LabelPrefix: labelPrefix}
	s, _ = h.volSilo(ns, "db")
	label := p.LabelFor(kplugin.Volume{ID: s.Metadata.UID + "/logs", Name: "kvo-" + ns + "-db-logs-" + s.Metadata.UID[:5]})
	squat, err := h.b.API.CreateVolume(ctx, linodego.VolumeCreateOptions{Label: label, Region: h.b.Region, Size: 10, Tags: []string{h.b.Tag}})
	if err != nil {
		t.Fatalf("a volume labelled %s: %v", label, err)
	}
	t.Logf("made Linode volume %d labelled %s by hand (no identity tag)", squat.ID, label)
	h.apply(t, dbYAML(ns, "data", "wal", "logs"))
	var refusal string
	eventually(t, "logs refused: its label is taken", 5*time.Minute, func() (bool, string) {
		_, out := h.volSilo(ns, "db")
		d, _ := h.c.Kvo("", "describe", "silo", "db", "-n", ns)
		if i := strings.Index(out+d, "not adopting"); i >= 0 {
			refusal = (out + d)[max(0, i-200):min(len(out+d), i+60)]
			return true, ""
		}
		return false, firstLine(d)
	})
	t.Logf("refused: …%s…", strings.ReplaceAll(refusal, "\n", " "))
	if v, err := h.b.API.GetVolume(ctx, squat.ID); err != nil || v.LinodeID != nil || len(v.Tags) != 1 {
		t.Errorf("the hand-made volume was touched: %+v %v", v, err)
	}
	if n := len(h.runVolumes(t)); n != 3 {
		t.Errorf("%d Linode volumes (want data's, wal's and the hand-made one: nothing made beside it)", n)
	}
	eventually(t, "the hand-made volume deleted (once Linode has it active)", 3*time.Minute, func() (bool, string) {
		err := h.b.API.DeleteVolume(ctx, squat.ID)
		return err == nil, fmt.Sprint(err)
	})
	eventually(t, "logs made once the label is free", 10*time.Minute, func() (bool, string) {
		s, _ = h.volSilo(ns, "db")
		ok := s.Status.Phase == "Running" && len(s.Status.Volumes) == 3
		for _, v := range s.Status.Volumes {
			ok = ok && v.Mounted
		}
		return ok, fmt.Sprintf("%+v", s.Status.Volumes)
	})
	t.Log("once the hand-made volume was gone, logs got a volume of its own")

	// wal out of the spec: its Linode volume deleted, data's kept (K-035's scenario on real volumes).
	h.apply(t, dbYAML(ns, "data", "logs"))
	walID := strings.Split(handles["wal"], ":")[0]
	// A container's mounts can't change: core keeps wal mounted until the container is made again,
	// and says so (Updated=False, VolumeRemovalWaits, naming wal); kvo reset makes it again.
	eventually(t, "db says wal waits for the container to be made again", 3*time.Minute, func() (bool, string) {
		s, out := h.volSilo(ns, "db")
		for _, c := range s.Status.Conditions {
			if c.Type == "Updated" && c.Status == "False" && c.Reason == "VolumeRemovalWaits" && strings.Contains(c.Message, "wal") {
				t.Logf("db: Updated False VolumeRemovalWaits: %s", c.Message)
				return true, ""
			}
		}
		return false, firstLine(out) + fmt.Sprintf(" %+v", s.Status.Conditions)
	})
	t.Log(h.c.MustKvo("", "reset", "silo", "db", "-n", ns))
	walGone, last := false, ""
	for deadline := time.Now().Add(8 * time.Minute); time.Now().Before(deadline) && !walGone; time.Sleep(10 * time.Second) {
		walGone, last = func() (bool, string) {
			now := h.runVolumes(t)
			var ids []string
			for _, v := range now {
				ids = append(ids, fmt.Sprintf("%d attached=%v", v.ID, v.LinodeID != nil))
			}
			s, _ := h.volSilo(ns, "db")
			var mounts string
			if s.Status.Host != "" {
				mounts, _ = h.sh(s.Status.Host, "findmnt -rn -o TARGET | grep '/wal$' || echo 'wal not mounted'")
			}
			inStatus := false
			for _, v := range s.Status.Volumes {
				inStatus = inStatus || v.Name == "wal"
			}
			return !strings.Contains(fmt.Sprint(ids), walID+" ") && strings.Contains(mounts, "wal not mounted") && !inStatus,
				fmt.Sprintf("Linode %v; status.volumes %+v; on %s: %s", ids, s.Status.Volumes, s.Status.Host, strings.TrimSpace(mounts))
		}()
	}
	if !walGone {
		t.Errorf("wal dropped and db reset: not unmounted, let go and deleted in 8 minutes: %s", last)
	}
	if got := h.c.MustKvo("", "exec", "silo", "db", "-n", ns, "--", "cat", "/data/m"); strings.TrimSpace(got) != "d" {
		t.Errorf("data after wal's removal: %q", got)
	}
	if walGone {
		t.Log("wal dropped, db reset: unmounted on the worker, gone from status, its Linode volume detached and deleted; data's kept with its data")
	}

	// Deleted with the silo, and only then.
	before := h.runVolumes(t)
	time.Sleep(30 * time.Second)
	if now := h.runVolumes(t); len(now) != len(before) {
		t.Errorf("volumes changed with nothing asked: %v → %v", keys(before), keys(now))
	}
	h.c.MustKvo("", "delete", "silo", "db", "-n", ns)
	eventually(t, "db's Linode volumes deleted with it", 10*time.Minute, func() (bool, string) {
		now := h.runVolumes(t)
		return len(now) == 0, fmt.Sprint(keys(now))
	})
	t.Log("db deleted: its Linode volumes went with it")
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// closedFromHere says whether a TCP connect from this Mac to addr fails (refused or dropped).
func closedFromHere(addr string) (bool, string) {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return true, "dropped"
		}
		return true, "refused"
	}
	_ = c.Close()
	return false, "open"
}

// pfRules: what the model opens answers from here; nothing else does.
func pfRules(t *testing.T, h *ha3) {
	const ns = "rules"
	h.namespace(t, ns)
	h.apply(t, siloYAML(ns, "open", "w1", "ports: [{ name: http, port: 80 }]", "service:", "  ports: [{ port: http, hostPort: [external], as: 18080 }]")+"---\n"+
		siloYAML(ns, "inner", "w1", "ports: [{ name: http, port: 80 }]", "service:", "  ports: [{ port: http, hostPort: [internal], as: 18096 }]")+"---\n"+
		siloYAML(ns, "quiet", "w2", "ports: [{ name: http, port: 80 }]"))
	h.waitRunning(t, ns, 10*time.Minute, "open", "inner", "quiet")
	w1 := h.ms["w1"]
	eventually(t, "[external] port 18080 answers this Mac", 2*time.Minute, func() (bool, string) {
		body, err := get(fmt.Sprintf("http://%s:18080/", w1.Public))
		return err == nil && strings.Contains(body, "nginx"), fmt.Sprint(err)
	})
	if ok, how := closedFromHere(net.JoinHostPort(w1.Public, "18096")); !ok {
		t.Errorf("[internal] port 18096 answers on the public address: %s", how)
	}
	if out, err := h.sh("w2", fmt.Sprintf("curl -sS --max-time 5 http://%s:18096/ | head -c 200", w1.Private)); err != nil || !strings.Contains(out, "nginx") {
		t.Errorf("[internal] port 18096 from w2 over the private network: %v %s", err, out)
	}
	t.Log("[external] answers from here; [internal] answers from w2 on the private address, not on the public one")

	// Every TCP listener on every machine, from here: only SSH, the API on the control plane, and
	// the one [external] port answer.
	allowed := map[string][]string{"cp": {"22", "6443"}, "w1": {"22", "18080"}, "w2": {"22"}}
	for _, n := range []string{"cp", "w1", "w2"} {
		m := h.ms[n]
		out := h.mustSh(t, n, "ss -Hltn | awk '{print $4}'")
		ports := map[string]bool{}
		for _, a := range strings.Fields(out) {
			host, port, err := net.SplitHostPort(a)
			if err != nil || strings.HasPrefix(host, "127.") || host == "[::1]" || host == "::1" {
				continue
			}
			ports[port] = true
		}
		var open, closed []string
		for port := range ports {
			ok, how := closedFromHere(net.JoinHostPort(m.Public, port))
			switch {
			case !ok && !slices.Contains(allowed[n], port):
				t.Errorf("%s: port %d is listening and answers this Mac; the model closes it", n, mustAtoi(port))
				open = append(open, port)
			case !ok:
				open = append(open, port)
			default:
				closed = append(closed, port+"("+how+")")
			}
		}
		slices.Sort(open)
		slices.Sort(closed)
		t.Logf("%s (%s): listening and open from here %v; listening and closed from here %v", n, m.Public, open, closed)
	}
	// The silo's own port without hostPort, and the container's port, aren't reachable either.
	if ok, how := closedFromHere(net.JoinHostPort(h.ms["w2"].Public, "80")); !ok {
		t.Errorf("quiet's port 80 answers on w2's public address: %s", how)
	}
}

func mustAtoi(s string) int { n, _ := strconv.Atoi(s); return n }
