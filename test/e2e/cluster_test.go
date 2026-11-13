//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// cluster is a control plane and two docker workers on Linodes, installed with kvoadm, with this
// provider's plugin (controller side on the control plane, node side on the workers), and the
// CLIs for here.
type cluster struct {
	b                            *bed
	ctx                          context.Context
	kvo, kvoadm, kevasod, plugin string
	env                          []string
	ssh                          []string // kvoadm's SSH flags
	cp, w1, w2                   machine
}

// sweepAtEnd deletes everything the bed made when the test ends (unless KEVASO_E2E_KEEP and it
// failed), and checks nothing tagged is left.
func (b *bed) sweepAtEnd(t *testing.T) {
	t.Cleanup(func() {
		if b.state != "" {
			t.Logf("KEEPING the cluster in %s (tag %s): KEVASO_E2E_SWEEP=%s go test -tags e2e -run TestSweep deletes it", b.state, b.tag, b.tag)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		if os.Getenv("KEVASO_E2E_KEEP") != "" && t.Failed() {
			t.Logf("KEEPING everything tagged %s", b.tag)
			return
		}
		swept := b.sweep(ctx)
		t.Logf("swept: %s", strings.Join(swept, ", "))
		for i := 0; i < 20; i++ {
			left := b.remaining(ctx)
			if len(left) == 0 {
				t.Logf("nothing tagged %s is left in the account", b.tag)
				return
			}
			time.Sleep(15 * time.Second)
			b.sweep(ctx)
		}
		t.Errorf("still tagged %s after cleanup: %v", b.tag, b.remaining(ctx))
	})
}

// build builds a package for here or for the Linodes.
func build(t *testing.T, ctx context.Context, dir, out, pkg string, linux bool) string {
	t.Helper()
	p := filepath.Join(dir, out)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", p, pkg)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if linux {
		cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
	} else {
		cmd.Env = append(cmd.Env, "GOOS="+runtime.GOOS)
	}
	if o, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, o)
	}
	return p
}

func newCluster(t *testing.T, ctx context.Context, b *bed) *cluster {
	bin := t.TempDir()
	c := &cluster{b: b, ctx: ctx,
		kvoadm:  build(t, ctx, bin, "kvoadm", "kevaso.com/core/cmd/kvoadm", false),
		kvo:     build(t, ctx, bin, "kvo", "kevaso.com/core/cmd/kvo", false),
		kevasod: build(t, ctx, bin, "kevasod", "kevaso.com/core/cmd/kevasod", true),
		plugin:  build(t, ctx, bin, "kevaso-provider-linode", "kevaso.com/provider-linode/cmd/kevaso-provider-linode", true),
		ssh:     []string{"--ssh-key", b.key, "--accept-host-key-on-first-use"},
	}
	home := t.TempDir()
	if b.state != "" {
		home = filepath.Join(b.state, "home")
	}
	c.env = append(os.Environ(), "HOME="+home, "KVO_CONFIG="+filepath.Join(home, ".kevaso", "config.yaml"), "KVO_SERVER=", "KVO_TOKEN=")
	if ms, ok := b.restore(t); ok {
		c.ssh = []string{"--ssh-key", b.key, "--accept-host-key-on-first-use"}
		c.cp, c.w1, c.w2 = ms["cp"], ms["w1"], ms["w2"]
		t.Logf("reusing the cluster in %s: cp %s, w1 %s, w2 %s", b.state, c.cp.Public, c.w1.Public, c.w2.Public)
		c.diagnoseOnFailure(t)
		c.redeploy(t)
		return c
	}

	type made struct {
		role string
		m    machine
	}
	ch := make(chan made, 3)
	for _, role := range []string{"cp", "w1", "w2"} {
		go func() { ch <- made{role, b.create(ctx, role)} }()
	}
	ms := map[string]machine{}
	for i := 0; i < 3; i++ {
		x := <-ch
		ms[x.role] = x.m
	}
	if t.Failed() {
		t.FailNow()
	}
	c.cp, c.w1, c.w2 = ms["cp"], ms["w1"], ms["w2"]
	b.save(t, ms)
	c.diagnoseOnFailure(t)
	c.install(t)
	return c
}

// diagnoseOnFailure logs, when the test failed, what the hosts and the cluster were doing.
func (c *cluster) diagnoseOnFailure(t *testing.T) {
	b := c.b
	t.Cleanup(func() { // runs before the sweep: what was going on, if it failed
		if !t.Failed() {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for name, m := range map[string]machine{"cp": c.cp, "w1": c.w1, "w2": c.w2} {
			out, _ := b.ssh(dctx, m, "journalctl -u kevasod -n 80 --no-pager 2>&1 | grep -iv 'heartbeat' | tail -60; docker ps -a 2>&1 | head; nft list ruleset 2>&1 | grep -i 'kevaso\\|dnat\\|reject' | head -40")
			t.Logf("── %s ──\n%s", name, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "wide"}, {"get", "silos", "-A", "-o", "yaml"}, {"get", "leases", "-A"}} {
			cmd := exec.CommandContext(dctx, c.kvo, args...)
			cmd.Env = c.env
			out, _ := cmd.CombinedOutput()
			t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})
}

// install makes the cluster on fresh machines.
func (c *cluster) install(t *testing.T) {
	b := c.b
	out := c.must(t, "", c.kvoadm, append([]string{"init", "root@" + c.cp.Public, "--kevasod", c.kevasod, "--name", "cp", "--cluster", "e2e",
		"--api-server", "https://" + c.cp.Public + ":6443"}, c.ssh...)...)
	if !regexp.MustCompile(`KVR1-`).MatchString(out) {
		t.Fatalf("no recovery key printed:\n%s", out)
	}
	for name, m := range map[string]machine{"w1": c.w1, "w2": c.w2} {
		t.Log(c.must(t, "", c.kvoadm, append([]string{"host", "add", "root@" + m.Public, "--kevasod", c.kevasod, "--name", name,
			"--install", "worker", "--runtime", "docker", "--region", b.region}, c.ssh...)...))
	}
	cfgFile := filepath.Join(t.TempDir(), "linode.json")
	_ = os.WriteFile(cfgFile, []byte(fmt.Sprintf(`{"tags": [%q], "labelPrefix": %q}`, b.tag, b.tag+"-")), 0o644)
	t.Log(c.must(t, "", c.kvoadm, append([]string{"plugin", "install", "root@" + c.cp.Public, "--name", "linode", "--binary", c.plugin,
		"--credential", "token=" + os.Getenv("KEVASO_LINODE_TOKEN_FILE"), "--config", cfgFile}, c.ssh...)...))
	nodeCfg := filepath.Join(t.TempDir(), "node.json")
	_ = os.WriteFile(nodeCfg, []byte(fmt.Sprintf(`{"region": %q}`, b.region)), 0o644)
	for _, w := range []machine{c.w1, c.w2} {
		t.Log(c.must(t, "", c.kvoadm, append([]string{"plugin", "install", "root@" + w.Public, "--name", "linode", "--binary", c.plugin,
			"--side", "node", "--config", nodeCfg}, c.ssh...)...))
	}
	c.eventually(t, "the workers Ready and recognised as Linodes", 4*time.Minute, func() (bool, string) {
		var seen []string
		n := 0
		for _, w := range []string{"w1", "w2"} {
			out, err := c.sh("", c.kvo, "get", "host", w, "-o", "json")
			var h struct {
				Status struct {
					Provider *struct{ Name string } `json:"provider"`
				} `json:"status"`
			}
			if err == nil && json.Unmarshal([]byte(out), &h) == nil && h.Status.Provider != nil && h.Status.Provider.Name == "linode" {
				n++
			}
			seen = append(seen, w+": "+firstLine(out))
		}
		return n == 2, strings.Join(seen, "; ")
	})
	c.must(t, "apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: e2e }\n", c.kvo, "apply", "-f", "-")
}

func (c *cluster) cmd(stdin string, extraEnv []string, name string, args ...string) *exec.Cmd {
	x := exec.CommandContext(c.ctx, name, args...)
	x.Env, x.Stdin = append(append([]string(nil), c.env...), extraEnv...), strings.NewReader(stdin)
	return x
}

func (c *cluster) sh(stdin string, name string, args ...string) (string, error) {
	out, err := c.cmd(stdin, nil, name, args...).CombinedOutput()
	return string(out), err
}

func (c *cluster) must(t *testing.T, stdin string, name string, args ...string) string {
	t.Helper()
	out, err := c.sh(stdin, name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
	return out
}

// inE2E puts -n e2e among kvo's flags: before any "--", after which everything is the command's.
func inE2E(args []string) []string {
	for i, a := range args {
		if a == "--" {
			return append(append(append([]string(nil), args[:i]...), "-n", "e2e"), args[i:]...)
		}
	}
	return append(append([]string(nil), args...), "-n", "e2e")
}

// kvoe runs kvo in the e2e namespace.
func (c *cluster) kvoe(args ...string) (string, error) {
	return c.sh("", c.kvo, inE2E(args)...)
}

func (c *cluster) mustKvo(t *testing.T, args ...string) string {
	t.Helper()
	return c.must(t, "", c.kvo, inE2E(args)...)
}

func (c *cluster) apply(t *testing.T, yaml string) {
	t.Helper()
	c.must(t, yaml, c.kvo, "apply", "-n", "e2e", "-f", "-")
}

func (c *cluster) eventually(t *testing.T, what string, d time.Duration, f func() (bool, string)) {
	t.Helper()
	deadline, last := time.Now().Add(d), ""
	for time.Now().Before(deadline) {
		ok, s := f()
		if ok {
			return
		}
		last = s
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s: not in %v; last: %s", what, d, last)
}

// siloInfo is what the tests read of a silo.
type siloInfo struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Phase      string `json:"phase"`
		Host       string `json:"host"`
		MovingFrom string `json:"movingFrom"`
		Movable    string `json:"movable"`
		Volumes    []struct {
			Name, Handle, AttachedTo string
			Mounted                  bool
		} `json:"volumes"`
		Conditions []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func (s siloInfo) cond(typ string) (status, reason string) {
	for _, c := range s.Status.Conditions {
		if c.Type == typ {
			return c.Status, c.Reason
		}
	}
	return "", ""
}

func (c *cluster) silo(name string) (siloInfo, error) {
	var s siloInfo
	out, err := c.kvoe("get", "silo", name, "-o", "json")
	if err != nil {
		return s, fmt.Errorf("%v: %s", err, firstLine(out))
	}
	return s, json.Unmarshal([]byte(out), &s)
}

// running waits for a silo to run, and returns it.
func (c *cluster) running(t *testing.T, name string, d time.Duration) siloInfo {
	t.Helper()
	var s siloInfo
	c.eventually(t, name+" running", d, func() (bool, string) {
		var err error
		s, err = c.silo(name)
		return err == nil && s.Status.Phase == "Running" && s.Status.MovingFrom == "", fmt.Sprintf("%+v %v", s.Status, err)
	})
	return s
}

func (c *cluster) host(name string) machine {
	return map[string]machine{"cp": c.cp, "w1": c.w1, "w2": c.w2}[name]
}

func other(worker string) string {
	if worker == "w1" {
		return "w2"
	}
	return "w1"
}

// fresh deletes what an earlier run on this cluster left (a reused cluster), and waits until it's gone.
func (c *cluster) fresh(t *testing.T, kind string, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := c.kvoe("get", kind, n); err != nil {
			continue
		}
		t.Logf("deleting %s %s left from an earlier run", kind, n)
		_, _ = c.kvoe("delete", kind, n)
		c.eventually(t, kind+" "+n+" gone", 6*time.Minute, func() (bool, string) {
			_, err := c.kvoe("get", kind, n)
			return err != nil, ""
		})
	}
}

// redeploy puts this build's kevasod on a reused cluster's hosts (where it differs) and restarts
// it; silos keep running.
func (c *cluster) redeploy(t *testing.T) {
	want := sum(c.kevasod)
	for _, name := range []string{"cp", "w1", "w2"} {
		m := c.host(name)
		if out, _ := c.b.ssh(c.ctx, m, "sha256sum /usr/local/bin/kevasod"); strings.HasPrefix(out, want) {
			continue
		}
		scp := exec.CommandContext(c.ctx, "scp", "-q", "-i", c.b.key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
			"-o", "LogLevel=ERROR", c.kevasod, "root@"+m.Public+":/usr/local/bin/kevasod.new")
		if out, err := scp.CombinedOutput(); err != nil {
			t.Fatalf("copy kevasod to %s: %v %s", name, err, out)
		}
		if out, err := c.b.ssh(c.ctx, m, "mv /usr/local/bin/kevasod.new /usr/local/bin/kevasod && systemctl restart kevasod"); err != nil {
			t.Fatalf("restart kevasod on %s: %v %s", name, err, out)
		}
		t.Logf("%s: kevasod replaced with this build", name)
	}
	c.eventually(t, "the workers Ready after the restart", 3*time.Minute, func() (bool, string) {
		out, _ := c.sh("", c.kvo, "get", "hosts")
		return strings.Count(out, "docker:Ready") == 2, out
	})
}
