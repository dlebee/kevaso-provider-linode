package linodetest

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

// Kit is what every Linode run needs on this side: the binaries, built once, and clusters to
// talk to, each with a home of its own (kvoadm's keys and the kvo config live there).
type Kit struct {
	T           *testing.T
	Ctx         context.Context
	Bin         string
	Kvo, Kvoadm string // for this Machine
}

func NewKit(t *testing.T, ctx context.Context) *Kit {
	k := &Kit{T: t, Ctx: ctx, Bin: t.TempDir()}
	k.Kvoadm = k.Build("kvoadm", "kevaso.com/core/cmd/kvoadm", runtime.GOOS, "")
	k.Kvo = k.Build("kvo", "kevaso.com/core/cmd/kvo", runtime.GOOS, "")
	return k
}

// Build builds pkg for goos ("linux": the Linodes, amd64) as bin/out, stamped with version when
// one is given.
func (k *Kit) Build(out, pkg, goos, version string) string {
	k.T.Helper()
	p := filepath.Join(k.Bin, out)
	args := []string{"build", "-o", p}
	if version != "" {
		args = append(args, "-ldflags", "-X kevaso.com/core/internal/version.Version="+version)
	}
	cmd := exec.CommandContext(k.Ctx, "go", append(args, pkg)...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if goos == "linux" {
		cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
	}
	if o, err := cmd.CombinedOutput(); err != nil {
		k.T.Fatalf("build %s: %v\n%s", pkg, err, o)
	}
	return p
}

// Cluster is one cluster as an operator here sees it: a home with kvoadm's keys and kvo's config.
type Cluster struct {
	K    *Kit
	Home string
	Cfg  string
	Env  []string
}

func (k *Kit) Cluster() *Cluster {
	home := k.T.TempDir()
	cfg := filepath.Join(home, ".kevaso", "config.yaml")
	return &Cluster{K: k, Home: home, Cfg: cfg, Env: append(os.Environ(), "HOME="+home, "KVO_CONFIG="+cfg, "KVO_SERVER=")}
}

func (c *Cluster) Run(stdin string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(c.K.Ctx, name, args...)
	cmd.Env, cmd.Stdin = c.Env, strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *Cluster) Must(stdin string, name string, args ...string) string {
	c.K.T.Helper()
	out, err := c.Run(stdin, name, args...)
	if err != nil {
		c.K.T.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
	return out
}

// Kvo runs kvo with the cluster's context (every server it lists, failing over).
func (c *Cluster) Kvo(stdin string, args ...string) (string, error) {
	return c.Run(stdin, c.K.Kvo, args...)
}

func (c *Cluster) MustKvo(stdin string, args ...string) string {
	c.K.T.Helper()
	return c.Must(stdin, c.K.Kvo, args...)
}

// Kvoadm runs kvoadm. A fresh Linode sometimes resets the first SSH connections (seen on the
// connection kvoadm opens only to read the host key, before it changes anything): those, and only
// those, are tried again.
func (c *Cluster) Kvoadm(args ...string) (string, error) {
	for attempt := 1; ; attempt++ {
		out, err := c.Run("", c.K.Kvoadm, args...)
		if err == nil || attempt == 3 || !strings.Contains(out, "couldn't read the host key") || !strings.Contains(out, "connection reset by peer") {
			return out, err
		}
		c.K.T.Logf("kvoadm %s: SSH reset while reading the host key, trying again (%d)", args[0], attempt)
		time.Sleep(10 * time.Second)
	}
}

func (c *Cluster) MustKvoadm(args ...string) string {
	c.K.T.Helper()
	out, err := c.Kvoadm(args...)
	if err != nil {
		c.K.T.Fatalf("kvoadm %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

var serverLine = regexp.MustCompile(`(?m)^(\s*server:\s*).*$`)

// Servers is what the kvo context lists.
func (c *Cluster) Servers() string {
	b, _ := os.ReadFile(c.Cfg)
	m := serverLine.FindStringSubmatch(string(b))
	if m == nil {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(m[0], m[1]))
}

// KvoAt runs kvo against one API server only: a copy of the config with that server alone.
func (c *Cluster) KvoAt(url string, args ...string) (string, error) {
	b, err := os.ReadFile(c.Cfg)
	if err != nil {
		return "", err
	}
	one := filepath.Join(filepath.Dir(c.Cfg), "at-"+regexp.MustCompile(`[^0-9a-z]+`).ReplaceAllString(url, "_")+".yaml")
	if err := os.WriteFile(one, []byte(serverLine.ReplaceAllString(string(b), "${1}"+url)), 0o600); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(c.K.Ctx, c.K.Kvo, args...)
	cmd.Env = append(append([]string(nil), c.Env...), "KVO_CONFIG="+one)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// HostView is the part of a Host the tests read.
type HostView struct {
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

func (h HostView) Ready() bool {
	for _, c := range h.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

func (h HostView) Member() string {
	if h.Status.ControlPlane == nil {
		return ""
	}
	return h.Status.ControlPlane.Member
}

func (h HostView) Leader() bool { return h.Status.ControlPlane != nil && h.Status.ControlPlane.Leader }

func (h HostView) APIURL() string {
	if h.Status.ControlPlane == nil {
		return ""
	}
	return h.Status.ControlPlane.APIURL
}

// DecodeList reads kvo get -o json: one object alone, or an array.
func DecodeList[T any](out string) ([]T, error) {
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

// Hosts lists the Hosts by name (through every server the context lists).
func (c *Cluster) Hosts() (map[string]HostView, error) {
	out, err := c.Kvo("", "get", "hosts", "-o", "json")
	if err != nil {
		return nil, err
	}
	l, err := DecodeList[HostView](out)
	if err != nil {
		return nil, err
	}
	m := map[string]HostView{}
	for _, h := range l {
		m[h.Metadata.Name] = h
	}
	return m, nil
}

// Eventually polls f every 5s until it says yes, or fails the test after d with what f said last.
func Eventually(t *testing.T, what string, d time.Duration, f func() (bool, string)) {
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

// SSHFlags are kvoadm's flags to log in to the bed's machines.
func (b *Bed) SSHFlags() []string {
	return []string{"--ssh-key", b.Key, "--accept-host-key-on-first-use"}
}
