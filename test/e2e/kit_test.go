//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// kit is what every Linode run needs on this side: the binaries, built once, and clusters to
// talk to, each with a home of its own (kvoadm's keys and the kvo config live there).
type kit struct {
	t           *testing.T
	ctx         context.Context
	bin         string
	kvo, kvoadm string // for this machine
}

func newKit(t *testing.T, ctx context.Context) *kit {
	k := &kit{t: t, ctx: ctx, bin: t.TempDir()}
	k.kvoadm = k.build("kvoadm", "kevaso.com/core/cmd/kvoadm", runtime.GOOS, "")
	k.kvo = k.build("kvo", "kevaso.com/core/cmd/kvo", runtime.GOOS, "")
	return k
}

// build builds pkg for goos ("linux": the Linodes, amd64) as bin/out, stamped with version when
// one is given.
func (k *kit) build(out, pkg, goos, version string) string {
	k.t.Helper()
	p := filepath.Join(k.bin, out)
	args := []string{"build", "-o", p}
	if version != "" {
		args = append(args, "-ldflags", "-X kevaso.com/core/internal/version.Version="+version)
	}
	cmd := exec.CommandContext(k.ctx, "go", append(args, pkg)...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if goos == "linux" {
		cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
	}
	if o, err := cmd.CombinedOutput(); err != nil {
		k.t.Fatalf("build %s: %v\n%s", pkg, err, o)
	}
	return p
}

// cluster is one cluster as an operator here sees it: a home with kvoadm's keys and kvo's config.
type cluster struct {
	k    *kit
	home string
	cfg  string
	env  []string
}

func (k *kit) cluster() *cluster {
	home := k.t.TempDir()
	cfg := filepath.Join(home, ".kevaso", "config.yaml")
	return &cluster{k: k, home: home, cfg: cfg, env: append(os.Environ(), "HOME="+home, "KVO_CONFIG="+cfg, "KVO_SERVER=")}
}

func (c *cluster) run(stdin string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(c.k.ctx, name, args...)
	cmd.Env, cmd.Stdin = c.env, strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *cluster) must(stdin string, name string, args ...string) string {
	c.k.t.Helper()
	out, err := c.run(stdin, name, args...)
	if err != nil {
		c.k.t.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
	return out
}

// kvo runs kvo with the cluster's context (every server it lists, failing over).
func (c *cluster) kvo(stdin string, args ...string) (string, error) {
	return c.run(stdin, c.k.kvo, args...)
}

func (c *cluster) mustKvo(stdin string, args ...string) string {
	c.k.t.Helper()
	return c.must(stdin, c.k.kvo, args...)
}

// kvoadm runs kvoadm. A fresh Linode sometimes resets the first SSH connections (seen on the
// connection kvoadm opens only to read the host key, before it changes anything): those, and only
// those, are tried again.
func (c *cluster) kvoadm(args ...string) (string, error) {
	for attempt := 1; ; attempt++ {
		out, err := c.run("", c.k.kvoadm, args...)
		if err == nil || attempt == 3 || !strings.Contains(out, "couldn't read the host key") || !strings.Contains(out, "connection reset by peer") {
			return out, err
		}
		c.k.t.Logf("kvoadm %s: SSH reset while reading the host key, trying again (%d)", args[0], attempt)
		time.Sleep(10 * time.Second)
	}
}

func (c *cluster) mustKvoadm(args ...string) string {
	c.k.t.Helper()
	out, err := c.kvoadm(args...)
	if err != nil {
		c.k.t.Fatalf("kvoadm %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

var serverLine = regexp.MustCompile(`(?m)^(\s*server:\s*).*$`)

// servers is what the kvo context lists.
func (c *cluster) servers() string {
	b, _ := os.ReadFile(c.cfg)
	m := serverLine.FindStringSubmatch(string(b))
	if m == nil {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(m[0], m[1]))
}

// kvoAt runs kvo against one API server only: a copy of the config with that server alone.
func (c *cluster) kvoAt(url string, args ...string) (string, error) {
	b, err := os.ReadFile(c.cfg)
	if err != nil {
		return "", err
	}
	one := filepath.Join(filepath.Dir(c.cfg), "at-"+regexp.MustCompile(`[^0-9a-z]+`).ReplaceAllString(url, "_")+".yaml")
	if err := os.WriteFile(one, []byte(serverLine.ReplaceAllString(string(b), "${1}"+url)), 0o600); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(c.k.ctx, c.k.kvo, args...)
	cmd.Env = append(append([]string(nil), c.env...), "KVO_CONFIG="+one)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// hostView is the part of a Host the tests read.
type hostView struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Address    string `json:"address"`
		Components struct {
			ControlPlane *struct {
				StateOnly bool `json:"stateOnly"`
			} `json:"controlPlane"`
			Worker *struct{} `json:"worker"`
		} `json:"components"`
	} `json:"spec"`
	Status struct {
		Version       string     `json:"version"`
		LastHeartbeat *time.Time `json:"lastHeartbeat"`
		ControlPlane  *struct {
			APIURL  string `json:"apiURL"`
			Store   string `json:"store"`
			PeerURL string `json:"peerURL"`
			Member  string `json:"member"`
			Leader  bool   `json:"leader"`
		} `json:"controlPlane"`
		Conditions []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func (h hostView) ready() bool {
	for _, c := range h.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

func (h hostView) member() string {
	if h.Status.ControlPlane == nil {
		return ""
	}
	return h.Status.ControlPlane.Member
}

func (h hostView) leader() bool { return h.Status.ControlPlane != nil && h.Status.ControlPlane.Leader }

func (h hostView) apiURL() string {
	if h.Status.ControlPlane == nil {
		return ""
	}
	return h.Status.ControlPlane.APIURL
}

// decodeList reads kvo get -o json: one object alone, or an array.
func decodeList[T any](out string) ([]T, error) {
	s := strings.TrimSpace(out)
	if strings.HasPrefix(s, "{") {
		var one T
		err := json.Unmarshal([]byte(s), &one)
		return []T{one}, err
	}
	var many []T
	err := json.Unmarshal([]byte(s), &many)
	return many, err
}

// hosts lists the Hosts by name (through every server the context lists).
func (c *cluster) hosts() (map[string]hostView, error) {
	out, err := c.kvo("", "get", "hosts", "-o", "json")
	if err != nil {
		return nil, err
	}
	l, err := decodeList[hostView](out)
	if err != nil {
		return nil, err
	}
	m := map[string]hostView{}
	for _, h := range l {
		m[h.Metadata.Name] = h
	}
	return m, nil
}

// eventually polls f every 5s until it says yes, or fails the test after d with what f said last.
func eventually(t *testing.T, what string, d time.Duration, f func() (bool, string)) {
	t.Helper()
	deadline, last := time.Now().Add(d), ""
	for time.Now().Before(deadline) {
		ok, s := f()
		if ok {
			return
		}
		last = s
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("%s: not in %v; last: %s", what, d, last)
}

// sshFlags are kvoadm's flags to log in to the bed's machines.
func (b *bed) sshFlags() []string {
	return []string{"--ssh-key", b.key, "--accept-host-key-on-first-use"}
}
