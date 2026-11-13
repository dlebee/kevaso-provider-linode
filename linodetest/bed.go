// Package linodetest runs end-to-end tests on real Linodes: a Bed makes the test's machines and
// deletes them again, a Cluster builds Kevaso and drives kvoadm and kvo against them. This
// provider's e2e test uses it, and so can other providers' (the JuiceFS one runs its backends and
// workers on Linodes).
//
// Every resource a Bed makes carries its tag (PREFIX + a run id) as a tag and as the start of its
// label, and Sweep deletes only what carries it.
package linodetest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linode/linodego"
	"golang.org/x/crypto/ssh"
)

// Bed is the Linode account, for the test's own machines: every resource it makes carries Tag,
// and Sweep deletes everything with that tag.
type Bed struct {
	T      testing.TB
	API    *linodego.Client
	Tag    string // PREFIX + run id: the tag and label prefix of all it makes
	Region string
	Key    string // private key file
	Pub    string // authorized_keys line
	// Tags are put on every Linode besides Tag (e.g. kevaso-e2e, to find all test machines).
	Tags []string
}

// NewBed reads the token from tokenFile (never printed) and makes a key pair for the run. prefix
// starts every tag and label (e.g. kevaso-e2e-); the region is KEVASO_E2E_LINODE_REGION or ca-central.
func NewBed(t testing.TB, tokenFile, prefix string) *Bed {
	c := Client(t, tokenFile)
	run := make([]byte, 4)
	_, _ = rand.Read(run)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	blk, err := ssh.MarshalPrivateKey(priv, "kevaso e2e")
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(blk), 0o600); err != nil {
		t.Fatal(err)
	}
	sp, _ := ssh.NewPublicKey(pub)
	region := os.Getenv("KEVASO_E2E_LINODE_REGION")
	if region == "" {
		region = "ca-central"
	}
	return &Bed{T: t, API: c, Tag: prefix + hex.EncodeToString(run), Region: region, Key: keyFile,
		Pub: strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))}
}

// Client is a Linode API client with the token in tokenFile.
func Client(t testing.TB, tokenFile string) *linodego.Client {
	b, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("token file: %v", err)
	}
	c := linodego.NewClient(&http.Client{Timeout: 60 * time.Second})
	c.SetToken(strings.TrimSpace(string(b)))
	c.SetRetries()
	return &c
}

// Machine is one Linode of the test.
type Machine struct {
	ID              int
	Label           string
	Public, Private string
}

// Create makes a nanode (Ubuntu 24.04, with a private IPv4) and waits until SSH answers.
func (b *Bed) Create(ctx context.Context, role string) Machine {
	return b.CreateType(ctx, role, "g6-nanode-1")
}

// CreateType makes a Linode of the given plan (Ubuntu 24.04, with a private IPv4) and waits until
// SSH answers.
func (b *Bed) CreateType(ctx context.Context, role, plan string) Machine {
	b.T.Helper()
	pw := make([]byte, 24)
	_, _ = rand.Read(pw)
	private := true
	label := b.Tag + "-" + role
	in, err := b.API.CreateInstance(ctx, linodego.InstanceCreateOptions{Region: b.Region, Type: plan, Label: label,
		Image: "linode/ubuntu24.04", RootPass: hex.EncodeToString(pw) + "Aa1!", AuthorizedKeys: []string{b.Pub},
		PrivateIP: private, Tags: append([]string{b.Tag}, b.Tags...)})
	if err != nil {
		b.T.Errorf("create %s: %v", role, err)
		return Machine{}
	}
	m := Machine{ID: in.ID, Label: label}
	for _, ip := range in.IPv4 {
		if ip.IsPrivate() {
			m.Private = ip.String()
		} else {
			m.Public = ip.String()
		}
	}
	b.T.Logf("Linode %s (%d) at %s, private %s", label, m.ID, m.Public, m.Private)
	if _, err := b.API.WaitForInstanceStatus(ctx, in.ID, linodego.InstanceRunning, 300); err != nil {
		b.T.Errorf("%s never ran: %v", role, err)
		return m
	}
	for deadline := time.Now().Add(5 * time.Minute); ; {
		if out, err := b.SSH(ctx, m, "true"); err == nil {
			break
		} else if time.Now().After(deadline) {
			b.T.Errorf("%s: no SSH: %v %s", role, err, out)
			return m
		}
		time.Sleep(5 * time.Second)
	}
	// First boot runs apt on its own: let it finish before kvoadm installs anything.
	_, _ = b.SSH(ctx, m, "systemctl stop unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null; "+
		"for i in $(seq 120); do fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1 || break; sleep 2; done")
	return m
}

// SSH runs cmd as root on m and returns its combined output.
func (b *Bed) SSH(ctx context.Context, m Machine, cmd string) (string, error) {
	return b.SSHIn(ctx, m, "", cmd)
}

// SSHIn runs cmd as root on m with stdin (secrets go this way, never on a command line).
func (b *Bed) SSHIn(ctx context.Context, m Machine, stdin, cmd string) (string, error) {
	c := exec.CommandContext(ctx, "ssh", "-i", b.Key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "LogLevel=ERROR", "root@"+m.Public, cmd)
	c.Stdin = strings.NewReader(stdin)
	out, err := c.CombinedOutput()
	return string(out), err
}

// MustSSH is SSH that fails the test on an error.
func (b *Bed) MustSSH(ctx context.Context, m Machine, cmd string) string {
	b.T.Helper()
	out, err := b.SSH(ctx, m, cmd)
	if err != nil {
		b.T.Fatalf("%s: %s: %v\n%s", m.Label, firstLine(cmd), err, out)
	}
	return out
}

// Sweep deletes everything carrying the bed's tag, and says what it found.
func (b *Bed) Sweep(ctx context.Context) (left []string) {
	return SweepTag(ctx, b.API, b.Tag)
}

// SweepTag deletes every resource tagged tag (Linodes, volumes, NodeBalancers, domains); a token
// that can't list a kind of resource just skips it.
func SweepTag(ctx context.Context, api *linodego.Client, tag string) (left []string) {
	tagged := linodego.NewListOptions(0, fmt.Sprintf(`{"tags": %q}`, tag))
	nbs, _ := api.ListNodeBalancers(ctx, tagged)
	for _, nb := range nbs {
		left = append(left, fmt.Sprintf("NodeBalancer %d", nb.ID))
		_ = api.DeleteNodeBalancer(ctx, nb.ID)
	}
	ins, _ := api.ListInstances(ctx, tagged)
	for _, in := range ins {
		left = append(left, "Linode "+in.Label)
		_ = api.DeleteInstance(ctx, in.ID)
	}
	vols, _ := api.ListVolumes(ctx, tagged)
	for _, v := range vols {
		left = append(left, "volume "+v.Label)
		if v.LinodeID != nil {
			_ = api.DetachVolume(ctx, v.ID)
			_, _ = api.WaitForVolumeLinodeID(ctx, v.ID, nil, 180)
		}
		_ = api.DeleteVolume(ctx, v.ID)
	}
	ds, _ := api.ListDomains(ctx, nil)
	for _, d := range ds {
		for _, tg := range d.Tags {
			if tg == tag {
				left = append(left, "domain "+d.Domain)
				_ = api.DeleteDomain(ctx, d.ID)
			}
		}
	}
	return left
}

// Remaining lists what's still tagged (after a sweep: should be nothing).
func (b *Bed) Remaining(ctx context.Context) []string {
	tagged := linodego.NewListOptions(0, fmt.Sprintf(`{"tags": %q}`, b.Tag))
	var out []string
	ins, _ := b.API.ListInstances(ctx, tagged)
	for _, in := range ins {
		out = append(out, "Linode "+in.Label)
	}
	vols, _ := b.API.ListVolumes(ctx, tagged)
	for _, v := range vols {
		out = append(out, "volume "+v.Label)
	}
	nbs, _ := b.API.ListNodeBalancers(ctx, tagged)
	for _, nb := range nbs {
		out = append(out, fmt.Sprintf("NodeBalancer %d", nb.ID))
	}
	ds, _ := b.API.ListDomains(ctx, nil)
	for _, d := range ds {
		for _, tg := range d.Tags {
			if tg == b.Tag {
				out = append(out, "domain "+d.Domain)
			}
		}
	}
	return out
}

// CleanUp registers the end of the run: sweep the tag and check nothing is left, also when the
// test failed; KEVASO_E2E_KEEP=1 keeps a failed run's machines for a look.
func (b *Bed) CleanUp() {
	b.T.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		if os.Getenv("KEVASO_E2E_KEEP") != "" && b.T.Failed() {
			b.T.Logf("KEEPING everything tagged %s", b.Tag)
			return
		}
		swept := b.Sweep(ctx)
		b.T.Logf("swept: %s", strings.Join(swept, ", "))
		for i := 0; i < 20; i++ {
			left := b.Remaining(ctx)
			if len(left) == 0 {
				b.T.Logf("nothing tagged %s is left in the account", b.Tag)
				return
			}
			time.Sleep(15 * time.Second)
			b.Sweep(ctx)
		}
		b.T.Errorf("still tagged %s after cleanup: %v", b.Tag, b.Remaining(ctx))
	})
}

// LabelsWithPrefix lists the Linodes whose label starts with prefix (what a dead run may have left).
func LabelsWithPrefix(ctx context.Context, api *linodego.Client, prefix string) ([]linodego.Instance, error) {
	ins, err := api.ListInstances(ctx, nil)
	if err != nil {
		return nil, err
	}
	var out []linodego.Instance
	for _, in := range ins {
		if strings.HasPrefix(in.Label, prefix) {
			out = append(out, in)
		}
	}
	return out, nil
}

// Lookup asks Linode's own name server directly (a test zone isn't delegated).
func Lookup(ctx context.Context, name string) ([]string, error) {
	r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, "ns1.linode.com:53")
	}}
	return r.LookupHost(ctx, name)
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return l
}
