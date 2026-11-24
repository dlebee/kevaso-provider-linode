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

// TestMacWorkerJoinsALinodeCluster: this Mac is a worker of a cluster whose control plane is one
// Linode (core's ADR 0012). It proves the Mac joins through kvoadm host add (launchd, the signed
// darwin kevasod), reports the tart runtime, gets a mesh address on a utun, and that the Mac and
// the Linode reach each other over the mesh. With KEVASO_MAC_IMAGE (a macOS image already pulled)
// it also runs a macOS silo and execs in it.
//
// It creates one Linode, deletes it at the end (the bed's sweep, by tag) and removes kevasod from
// the Mac. Needs, on the Mac: Remote Login, a key in KEVASO_MAC_SSH_KEY authorized for
// KEVASO_MAC_SSH_USER, and passwordless sudo for that user (core's test/e2e/mac-setup.sh does the
// three, and its teardown undoes them). The binaries are built by the caller from the core branch
// under test (the workspace's core may be another):
//
//	KEVASO_LINODE_TOKEN_FILE=… KEVASO_MAC_SSH_USER=me KEVASO_MAC_SSH_KEY=~/.ssh/kevaso-mac-test \
//	KEVASO_MAC_KVOADM=… KEVASO_MAC_KVO=… KEVASO_MAC_KEVASOD_DARWIN=… KEVASO_MAC_KEVASOD_LINUX=… \
//	go test -tags e2e -count=1 -v -timeout 40m -run TestMacWorker ./test/e2e/
func TestMacWorkerJoinsALinodeCluster(t *testing.T) {
	need := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Skip(k + " not set")
		}
		return v
	}
	tokenFile, user, key := need("KEVASO_LINODE_TOKEN_FILE"), need("KEVASO_MAC_SSH_USER"), need("KEVASO_MAC_SSH_KEY")
	kvoadm, kvo, kdarwin, klinux := need("KEVASO_MAC_KVOADM"), need("KEVASO_MAC_KVO"), need("KEVASO_MAC_KEVASOD_DARWIN"), need("KEVASO_MAC_KEVASOD_LINUX")
	image := os.Getenv("KEVASO_MAC_IMAGE")

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-mac-")
	b.CleanupAtEnd(t)
	t.Cleanup(func() { macRemove(t) }) // runs before the bed's sweep

	lan := macLAN(t)
	cp := b.Create(ctx, "cp")
	k := &kit{T: t, Ctx: ctx, Bin: t.TempDir(), Kvoadm: kvoadm, Kvo: kvo}
	c := k.Cluster()

	out := c.MustKvoadm(append([]string{"init", "root@" + cp.Public, "--external-ip", cp.Public, "--kevasod", klinux, "--name", "cp", "--cluster", "mac",
		"--api-server", "https://" + cp.Public + ":6443"}, b.SSHFlags()...)...)
	if !strings.Contains(out, "KVR1-") {
		t.Fatalf("no recovery key printed:\n%s", out)
	}
	t.Log(c.MustKvoadm("host", "add", user+"@127.0.0.1", "--name", "mac", "--kevasod", kdarwin, "--internal-ip", lan, "--behind-nat",
		"--ssh-key", key, "--accept-host-key-on-first-use"))

	type host struct {
		Status struct {
			Runtimes map[string]struct {
				State    string
				Version  string
				Message  string
				MaxSilos int
			}
			Network struct {
				MeshIP string `json:"meshIP"`
			}
		}
	}
	var mac, cpHost host
	get := func(name string, into *host) (bool, string) {
		o, err := c.Kvo("", "get", "host", name, "-o", "json")
		if err != nil || json.Unmarshal([]byte(o), into) != nil {
			return false, o
		}
		return true, o
	}
	eventually(t, "the Mac Ready with tart, a mesh address and room for two silos", 6*time.Minute, func() (bool, string) {
		ok, o := get("mac", &mac)
		rt := mac.Status.Runtimes["tart"]
		return ok && rt.State == "Ready" && rt.MaxSilos == 2 && mac.Status.Network.MeshIP != "", o
	})
	if _, o := get("cp", &cpHost); cpHost.Status.Network.MeshIP == "" {
		t.Fatalf("the control plane has no mesh address:\n%s", o)
	}
	t.Logf("mesh: mac %s, cp %s; tart %s", mac.Status.Network.MeshIP, cpHost.Status.Network.MeshIP, mac.Status.Runtimes["tart"].Version)

	eventually(t, "the Mac pings the Linode over the mesh", 3*time.Minute, func() (bool, string) {
		o, err := exec.CommandContext(ctx, "ping", "-c", "2", "-t", "8", cpHost.Status.Network.MeshIP).CombinedOutput()
		return err == nil, string(o)
	})
	eventually(t, "the Linode pings the Mac over the mesh", 3*time.Minute, func() (bool, string) {
		o, err := b.SSH(ctx, cp, "ping -c 2 -W 3 "+mac.Status.Network.MeshIP)
		return err == nil, o
	})

	if image == "" {
		t.Log("KEVASO_MAC_IMAGE not set: no macOS silo run")
		return
	}
	c.MustKvo("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: mac }\n", "apply", "-f", "-")
	c.MustKvo("apiVersion: kevaso/v1\nkind: Silo\nmetadata: { name: ios, namespace: mac }\nspec:\n  runtime: tart\n  container: { image: "+image+" }\n  resources: { cpu: 4, memory: 8Gi }\n", "apply", "-f", "-")
	eventually(t, "the macOS silo Running", 12*time.Minute, func() (bool, string) {
		o, _ := c.Kvo("", "get", "silo", "ios", "-n", "mac", "-o", "json")
		return strings.Contains(o, `"phase": "Running"`), o
	})
	if o := c.MustKvo("", "exec", "-n", "mac", "silo", "ios", "--", "sw_vers", "-productName"); !strings.Contains(o, "macOS") {
		t.Fatalf("exec in the macOS silo: %q", o)
	}
	c.MustKvo("", "delete", "silo", "ios", "-n", "mac")
}

// macLAN is this Mac's address on its own network (the Host's --internal-ip: it is behind NAT).
func macLAN(t *testing.T) string {
	t.Helper()
	for _, ifc := range []string{"en0", "en1"} {
		if o, err := exec.Command("ipconfig", "getifaddr", ifc).Output(); err == nil && strings.TrimSpace(string(o)) != "" {
			return strings.TrimSpace(string(o))
		}
	}
	t.Fatal("no LAN address on en0 or en1")
	return ""
}

// macRemove takes kevasod off this Mac again: the daemon, its binary, config and data.
func macRemove(t *testing.T) {
	if t.Failed() { // the worker's own account of what happened, before it is removed
		if o, err := exec.Command("sudo", "-n", "sh", "-c", "tail -n 120 /var/log/kevasod.log; ls -la /var/lib/kevaso/tart 2>/dev/null; tail -n 40 /var/lib/kevaso/tart/*.log 2>/dev/null").CombinedOutput(); err == nil {
			t.Logf("kevasod's log on this Mac:\n%s", o)
		}
	}
	// The macOS silo VMs this run left (kevaso's are named kv-…), as this Mac's user.
	if o, err := exec.Command("tart", "list", "--format", "json").Output(); err == nil {
		var vms []struct{ Name string }
		_ = json.Unmarshal(o, &vms)
		for _, v := range vms {
			if strings.HasPrefix(v.Name, "kv-") {
				_ = exec.Command("tart", "stop", v.Name).Run()
				if o, err := exec.Command("tart", "delete", v.Name).CombinedOutput(); err != nil {
					t.Errorf("deleting VM %s: %v\n%s", v.Name, err, o)
				}
			}
		}
	}
	cmd := "launchctl bootout system/com.kevaso.fence 2>/dev/null; launchctl bootout system/com.kevaso.kevasod 2>/dev/null; " +
		"rm -f /Library/LaunchDaemons/com.kevaso.fence.plist /Library/LaunchDaemons/com.kevaso.kevasod.plist /usr/local/bin/kevasod /var/log/kevasod.log /var/log/kevaso-fence.log; " +
		"rm -rf /etc/kevaso /var/lib/kevaso /var/run/wireguard; true"
	if o, err := exec.Command("sudo", "-n", "sh", "-c", cmd).CombinedOutput(); err != nil {
		t.Errorf("removing kevasod from this Mac: %v\n%s", err, o)
	}
}
