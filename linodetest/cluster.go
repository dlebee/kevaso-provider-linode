package linodetest

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

// Cluster drives a Kevaso cluster on a Bed's machines from here: kvoadm and kvo built for this
// machine, kevasod for the Linodes, a HOME of the test's own (kvo's config, kvoadm's keys).
type Cluster struct {
	T       testing.TB
	Ctx     context.Context
	Bed     *Bed
	Bin     string // where the binaries are built
	Kvo     string
	Kvoadm  string
	Kevasod string
	Env     []string
	SSHArgs []string // kvoadm's SSH flags for the bed's key
}

// NewCluster builds kvo, kvoadm and kevasod (go.work picks core).
func NewCluster(ctx context.Context, t testing.TB, b *Bed) *Cluster {
	c := &Cluster{T: t, Ctx: ctx, Bed: b, Bin: t.TempDir()}
	c.Kvoadm = c.Build("kvoadm", "kevaso.com/core/cmd/kvoadm", runtime.GOOS)
	c.Kvo = c.Build("kvo", "kevaso.com/core/cmd/kvo", runtime.GOOS)
	c.Kevasod = c.Build("kevasod", "kevaso.com/core/cmd/kevasod", "linux")
	home := keepDir(t)
	c.Env = append(os.Environ(), "HOME="+home, "KVO_CONFIG="+filepath.Join(home, ".kevaso", "config.yaml"), "KVO_SERVER=")
	c.SSHArgs = []string{"--ssh-key", b.Key, "--accept-host-key-on-first-use"}
	return c
}

// Build builds pkg for goos (linux: amd64) into the cluster's bin folder.
func (c *Cluster) Build(out, pkg, goos string) string {
	c.T.Helper()
	p := filepath.Join(c.Bin, out)
	cmd := exec.CommandContext(c.Ctx, "go", "build", "-o", p, pkg)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if goos == "linux" {
		cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
	}
	if o, err := cmd.CombinedOutput(); err != nil {
		c.T.Fatalf("build %s: %v\n%s", pkg, err, o)
	}
	return p
}

// Sh runs a local program with the cluster's environment.
func (c *Cluster) Sh(stdin string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(c.Ctx, name, args...)
	cmd.Env, cmd.Stdin = c.Env, strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Must is Sh that fails the test on an error.
func (c *Cluster) Must(stdin string, name string, args ...string) string {
	c.T.Helper()
	out, err := c.Sh(stdin, name, args...)
	if err != nil {
		c.T.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
	return out
}

// KvoOK runs kvo, failing the test on an error.
func (c *Cluster) KvoOK(stdin string, args ...string) string {
	c.T.Helper()
	return c.Must(stdin, c.Kvo, args...)
}

// Eventually polls f every 5s until it says true, failing the test after d with f's last word.
func (c *Cluster) Eventually(what string, d time.Duration, f func() (bool, string)) {
	c.T.Helper()
	deadline, last := time.Now().Add(d), ""
	for time.Now().Before(deadline) {
		ok, s := f()
		if ok {
			return
		}
		last = s
		time.Sleep(5 * time.Second)
	}
	c.T.Fatalf("%s: not in %v; last: %s", what, d, last)
}

// Get unmarshals kvo get KIND NAME [-n NS] -o json into v; false if it isn't there (or kvo failed).
func (c *Cluster) Get(v any, kind, name, ns string) bool {
	args := []string{"get", kind, name, "-o", "json"}
	if ns != "" {
		args = append(args, "-n", ns)
	}
	out, err := c.Sh("", c.Kvo, args...)
	return err == nil && json.Unmarshal([]byte(out), v) == nil
}

// Init runs kvoadm init on cp (cluster e2e) and checks a recovery key was printed.
func (c *Cluster) Init(cp Machine, name string) {
	c.T.Helper()
	out := c.mustConnect(c.Kvoadm, append([]string{"init", "root@" + cp.Public, "--kevasod", c.Kevasod, "--name", name, "--cluster", "e2e",
		"--api-server", "https://" + cp.Public + ":6443"}, c.SSHArgs...)...)
	if !regexp.MustCompile(`KVR1-`).MatchString(out) {
		c.T.Fatalf("no recovery key printed:\n%s", out)
	}
}

// AddWorker runs kvoadm host add for a worker with runtime rt (docker, podman).
func (c *Cluster) AddWorker(m Machine, name, rt string) {
	c.T.Helper()
	c.T.Log(c.mustConnect(c.Kvoadm, append([]string{"host", "add", "root@" + m.Public, "--kevasod", c.Kevasod, "--name", name,
		"--install", "worker", "--runtime", rt, "--region", c.Bed.Region}, c.SSHArgs...)...))
}

// mustConnect is Must, tried again (8 times in all) when kvoadm couldn't even connect: a fresh
// Linode's sshd sometimes resets connections, before anything happened on it.
func (c *Cluster) mustConnect(name string, args ...string) string {
	c.T.Helper()
	for try := 1; ; try++ {
		out, err := c.Sh("", name, args...)
		if err == nil {
			return out
		}
		if try < 8 && strings.Contains(out, "couldn't read the host key") {
			c.T.Logf("%s: %s; again", filepath.Base(name), firstLine(out))
			time.Sleep(10 * time.Second)
			continue
		}
		c.T.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
}

// PluginInstall runs kvoadm plugin install on m with extra flags (--name, --binary, …).
func (c *Cluster) PluginInstall(m Machine, args ...string) string {
	c.T.Helper()
	out := c.mustConnect(c.Kvoadm, append(append([]string{"plugin", "install", "root@" + m.Public}, args...), c.SSHArgs...)...)
	c.T.Log(out)
	return out
}

// Diagnose logs, if the test failed, the hosts' and silos' state and each machine's journal for
// the given units (run it from a Cleanup registered after the machines exist).
func (c *Cluster) Diagnose(ms map[string]Machine, units ...string) {
	if !c.T.Failed() {
		return
	}
	dctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for name, m := range ms {
		var cmd strings.Builder
		cmd.WriteString("ls -la /run/kevaso-plugins/ /run/kevaso-plugins/* 2>&1; ")
		for _, u := range units {
			fmt.Fprintf(&cmd, "echo '── %s'; journalctl -u %s -n 40 --no-pager 2>&1; ", u, u)
		}
		cmd.WriteString("journalctl -u kevasod -n 80 --no-pager 2>&1 | grep -iv 'heartbeat' | tail -60")
		out, _ := c.Bed.SSH(dctx, m, cmd.String())
		c.T.Logf("── %s ──\n%s", name, out)
	}
	for _, args := range [][]string{{"get", "hosts", "-o", "yaml"}, {"get", "silos", "-A", "-o", "yaml"}} {
		cmd := exec.CommandContext(dctx, c.Kvo, args...)
		cmd.Env = c.Env
		out, _ := cmd.CombinedOutput()
		c.T.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
	}
}

// Parallel makes one Linode per role at once (plan by role, default nanode).
func (b *Bed) Parallel(ctx context.Context, plans map[string]string, roles ...string) map[string]Machine {
	type made struct {
		role string
		m    Machine
	}
	ch := make(chan made, len(roles))
	for _, role := range roles {
		plan := plans[role]
		if plan == "" {
			plan = "g6-nanode-1"
		}
		go func() { ch <- made{role, b.CreateType(ctx, role, plan)} }()
	}
	ms := map[string]Machine{}
	for range roles {
		x := <-ch
		ms[x.role] = x.m
	}
	if b.T.Failed() {
		b.T.FailNow()
	}
	return ms
}
