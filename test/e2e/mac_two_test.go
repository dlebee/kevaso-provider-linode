//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestTwoMacWorkersShareALinodeCluster: this Mac and a second Mac, on different networks, are
// workers of one cluster whose control plane is a Linode (core's ADR 0012, step 3). kvoadm reaches
// the second Mac over SSH at any address it can (a Tailscale address when the two are on separate
// networks); the cluster itself runs over Kevaso's mesh. It proves: both Macs Ready with tart, the
// two Macs reach each other over the mesh, four macOS silos spread two per Mac, a fifth waits for a
// slot and runs once one is deleted.
//
// Needs everything TestMacWorkerJoinsALinodeCluster needs (KEVASO_MAC_IMAGE is required here),
// plus, for the second Mac: KEVASO_MAC2_SSH (USER@ADDRESS), KEVASO_MAC2_SSH_KEY, and the same
// image pulled there; Remote Login, that key authorized and passwordless sudo for that user.
//
//	… KEVASO_MAC2_SSH=me@100.x.y.z KEVASO_MAC2_SSH_KEY=~/.ssh/kevaso-mac2-test \
//	go test -tags e2e -count=1 -v -timeout 70m -run TestTwoMacWorkers ./test/e2e/
func TestTwoMacWorkersShareALinodeCluster(t *testing.T) {
	need := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Skip(k + " not set")
		}
		return v
	}
	tokenFile, user, key := need("KEVASO_LINODE_TOKEN_FILE"), need("KEVASO_MAC_SSH_USER"), need("KEVASO_MAC_SSH_KEY")
	kvoadm, kvo, kdarwin, klinux := need("KEVASO_MAC_KVOADM"), need("KEVASO_MAC_KVO"), need("KEVASO_MAC_KEVASOD_DARWIN"), need("KEVASO_MAC_KEVASOD_LINUX")
	image, mac2, key2 := need("KEVASO_MAC_IMAGE"), need("KEVASO_MAC2_SSH"), need("KEVASO_MAC2_SSH_KEY")
	user2, addr2, _ := strings.Cut(mac2, "@")

	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Minute)
	defer cancel()
	// Its own deadline per call, not the test's: cleanup runs after the test's context is canceled.
	remote := func(cmd string) (string, error) {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer rcancel()
		o, err := exec.CommandContext(rctx, "ssh", "-i", key2, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
			"-o", "ConnectTimeout=15", mac2, cmd).CombinedOutput()
		return string(o), err
	}
	if o, err := remote("sudo -n true && /opt/homebrew/bin/tart list"); err != nil || !strings.Contains(o, strings.Split(image, ":")[0]) {
		t.Fatalf("the second Mac isn't ready (SSH, passwordless sudo, tart, the image %s): %v\n%s", image, err, o)
	}
	lan2 := ""
	for _, ifc := range []string{"en0", "en1"} {
		if o, err := remote("ipconfig getifaddr " + ifc); err == nil && strings.TrimSpace(o) != "" {
			lan2 = strings.TrimSpace(o)
			break
		}
	}
	if lan2 == "" {
		t.Fatal("no LAN address on the second Mac")
	}

	b := newBed(t, tokenFile, "kvt-mac2-")
	b.CleanupAtEnd(t)
	t.Cleanup(func() { macRemove(t) })
	t.Cleanup(func() { macRemoveRemote(t, remote) })

	lan := macLAN(t)
	cp := b.Create(ctx, "cp")
	k := &kit{T: t, Ctx: ctx, Bin: t.TempDir(), Kvoadm: kvoadm, Kvo: kvo}
	c := k.Cluster()
	out := c.MustKvoadm(append([]string{"init", "root@" + cp.Public, "--external-ip", cp.Public, "--kevasod", klinux, "--name", "cp", "--cluster", "macs",
		"--api-server", "https://" + cp.Public + ":6443"}, b.SSHFlags()...)...)
	if !strings.Contains(out, "KVR1-") {
		t.Fatalf("no recovery key printed:\n%s", out)
	}
	t.Log(c.MustKvoadm("host", "add", user+"@127.0.0.1", "--name", "mac", "--kevasod", kdarwin, "--internal-ip", lan, "--behind-nat",
		"--ssh-key", key, "--accept-host-key-on-first-use"))
	t.Log(c.MustKvoadm("host", "add", user2+"@"+addr2, "--name", "mac2", "--kevasod", kdarwin, "--internal-ip", lan2, "--behind-nat",
		"--ssh-key", key2, "--accept-host-key-on-first-use"))

	type host struct {
		Status struct {
			Runtimes map[string]struct {
				State    string
				MaxSilos int
			}
			Network struct {
				MeshIP string `json:"meshIP"`
			}
		}
	}
	mesh := map[string]string{}
	for _, name := range []string{"mac", "mac2"} {
		var h host
		eventually(t, name+" Ready with tart and a mesh address", 8*time.Minute, func() (bool, string) {
			o, err := c.Kvo("", "get", "host", name, "-o", "json")
			if err != nil || json.Unmarshal([]byte(o), &h) != nil {
				return false, o
			}
			rt := h.Status.Runtimes["tart"]
			return rt.State == "Ready" && rt.MaxSilos == 2 && h.Status.Network.MeshIP != "", o
		})
		mesh[name] = h.Status.Network.MeshIP
	}
	t.Logf("mesh: mac %s, mac2 %s", mesh["mac"], mesh["mac2"])

	// The two Macs, each behind its own router, reach each other over the mesh.
	eventually(t, "this Mac pings the second Mac over the mesh", 4*time.Minute, func() (bool, string) {
		o, err := exec.CommandContext(ctx, "ping", "-c", "2", "-t", "8", mesh["mac2"]).CombinedOutput()
		return err == nil, string(o)
	})
	eventually(t, "the second Mac pings this Mac over the mesh", 4*time.Minute, func() (bool, string) {
		o, err := remote("ping -c 2 -t 8 " + mesh["mac"])
		return err == nil, o
	})

	// Four silos, two per Mac; a fifth waits for a slot.
	c.MustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: mac }\n", "apply", "-f", "-")
	silo := func(name string) string {
		return "apiVersion: kevaso/v1\nkind: Silo\nmetadata: { name: " + name + ", namespace: mac }\nspec:\n  runtime: tart\n  container: { image: " + image + " }\n  resources: { cpu: 3, memory: 6Gi }\n"
	}
	names := []string{"s1", "s2", "s3", "s4"}
	apply := func(n string) { // the Nanode control plane can be slow to answer while silos start: retry
		var err error
		for i := 0; i < 5; i++ {
			if _, err = c.Kvo(silo(n), "apply", "-f", "-"); err == nil {
				return
			}
			time.Sleep(10 * time.Second)
		}
		t.Fatalf("kvo apply %s: %v", n, err)
	}
	for _, n := range names {
		apply(n)
	}
	type siloStatus struct {
		Status struct {
			Phase string `json:"phase"`
			Host  string `json:"host"`
		} `json:"status"`
	}
	get := func(n string) siloStatus {
		var s siloStatus
		o, _ := c.Kvo("", "get", "silo", n, "-n", "mac", "-o", "json")
		_ = json.Unmarshal([]byte(o), &s)
		return s
	}
	eventually(t, "four macOS silos Running", 20*time.Minute, func() (bool, string) {
		var state []string
		ok := true
		for _, n := range names {
			s := get(n)
			state = append(state, n+"="+s.Status.Phase+"@"+s.Status.Host)
			ok = ok && s.Status.Phase == "Running"
		}
		return ok, strings.Join(state, " ")
	})
	per := map[string]int{}
	for _, n := range names {
		per[get(n).Status.Host]++
	}
	if per["mac"] != 2 || per["mac2"] != 2 {
		t.Fatalf("four silos not spread two per Mac: %v", per)
	}
	if o := c.MustKvo("", "exec", "-n", "mac", "silo", "s3", "--", "sw_vers", "-productName"); !strings.Contains(o, "macOS") {
		t.Fatalf("exec in a silo: %q", o)
	}

	apply("s5")
	time.Sleep(45 * time.Second)
	if s := get("s5"); s.Status.Phase == "Running" || s.Status.Host != "" {
		t.Fatalf("a fifth macOS silo on two Macs is not held back: %+v", s)
	}
	victim := names[0]
	freed := get(victim).Status.Host
	c.MustKvo("", "delete", "silo", victim, "-n", "mac")
	eventually(t, "the fifth silo Running on the Mac with the free slot", 15*time.Minute, func() (bool, string) {
		s := get("s5")
		return s.Status.Phase == "Running" && s.Status.Host == freed, s.Status.Phase + "@" + s.Status.Host
	})

	for _, n := range []string{"s2", "s3", "s4", "s5"} {
		c.MustKvo("", "delete", "silo", n, "-n", "mac")
	}
	eventually(t, "the silos' VMs gone from both Macs", 6*time.Minute, func() (bool, string) {
		l, _ := exec.Command("tart", "list").Output()
		r, _ := remote("/opt/homebrew/bin/tart list")
		return !strings.Contains(string(l), "kv-mac-") && !strings.Contains(r, "kv-mac-"), string(l) + r
	})
}

// macRemoveRemote takes kevasod and the fence off the second Mac, and any kv- VM left there.
func macRemoveRemote(t *testing.T, remote func(string) (string, error)) {
	if t.Failed() {
		if o, err := remote("sudo -n tail -n 80 /var/log/kevasod.log"); err == nil {
			t.Logf("kevasod's log on the second Mac:\n%s", o)
		}
	}
	vms := "for v in $(/opt/homebrew/bin/tart list --format json | /usr/bin/python3 -c 'import json,sys; print(\" \".join(x[\"Name\"] for x in json.load(sys.stdin) if x[\"Name\"].startswith(\"kv-\")))'); do " +
		"for i in 1 2 3 4 5 6; do /opt/homebrew/bin/tart stop $v >/dev/null 2>&1; /opt/homebrew/bin/tart delete $v 2>/dev/null && break; sleep 10; done; done"
	if o, err := remote(vms); err != nil {
		t.Errorf("deleting VMs on the second Mac: %v\n%s", err, o)
	}
	cmd := "sudo -n sh -c 'launchctl bootout system/com.kevaso.fence 2>/dev/null; launchctl bootout system/com.kevaso.kevasod 2>/dev/null; " +
		"rm -f /Library/LaunchDaemons/com.kevaso.fence.plist /Library/LaunchDaemons/com.kevaso.kevasod.plist /usr/local/bin/kevasod /var/log/kevasod.log /var/log/kevaso-fence.log; " +
		"rm -rf /etc/kevaso /var/lib/kevaso /var/run/wireguard; true'"
	if o, err := remote(cmd); err != nil {
		t.Errorf("removing kevasod from the second Mac: %v\n%s", err, o)
	}
}
