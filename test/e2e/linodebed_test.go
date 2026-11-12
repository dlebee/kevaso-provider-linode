//go:build e2e

package e2e

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

// bed is the Linode account, for the test's own machines: every resource it makes carries tag,
// and sweep deletes everything with that tag.
type bed struct {
	t      *testing.T
	api    *linodego.Client
	tag    string
	region string
	key    string // private key file
	pub    string // authorized_keys line
}

func newBed(t *testing.T, tokenFile string) *bed {
	b, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("token file: %v", err)
	}
	c := linodego.NewClient(&http.Client{Timeout: 60 * time.Second})
	c.SetToken(strings.TrimSpace(string(b)))
	c.SetRetries()
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
	return &bed{t: t, api: &c, tag: "kevaso-e2e-" + hex.EncodeToString(run), region: region, key: keyFile,
		pub: strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))}
}

// machine is one Linode of the test.
type machine struct {
	ID              int
	Label           string
	Public, Private string
}

// create makes a nanode (Ubuntu 24.04, with a private IPv4) and waits until SSH answers.
func (b *bed) create(ctx context.Context, role string) machine {
	b.t.Helper()
	pw := make([]byte, 24)
	_, _ = rand.Read(pw)
	private := true
	label := b.tag + "-" + role
	in, err := b.api.CreateInstance(ctx, linodego.InstanceCreateOptions{Region: b.region, Type: "g6-nanode-1", Label: label,
		Image: "linode/ubuntu24.04", RootPass: hex.EncodeToString(pw) + "Aa1!", AuthorizedKeys: []string{b.pub},
		PrivateIP: private, Tags: []string{b.tag, "kevaso-e2e"}})
	if err != nil {
		b.t.Fatalf("create %s: %v", role, err)
	}
	m := machine{ID: in.ID, Label: label}
	for _, ip := range in.IPv4 {
		if ip.IsPrivate() {
			m.Private = ip.String()
		} else {
			m.Public = ip.String()
		}
	}
	b.t.Logf("Linode %s (%d) at %s, private %s", label, m.ID, m.Public, m.Private)
	if _, err := b.api.WaitForInstanceStatus(ctx, in.ID, linodego.InstanceRunning, 300); err != nil {
		b.t.Fatalf("%s never ran: %v", role, err)
	}
	for deadline := time.Now().Add(5 * time.Minute); ; {
		if out, err := b.ssh(ctx, m, "true"); err == nil {
			break
		} else if time.Now().After(deadline) {
			b.t.Fatalf("%s: no SSH: %v %s", role, err, out)
		}
		time.Sleep(5 * time.Second)
	}
	// First boot runs apt on its own: let it finish before kvoadm installs anything.
	_, _ = b.ssh(ctx, m, "systemctl stop unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null; "+
		"for i in $(seq 120); do fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1 || break; sleep 2; done")
	return m
}

func (b *bed) ssh(ctx context.Context, m machine, cmd string) (string, error) {
	c := exec.CommandContext(ctx, "ssh", "-i", b.key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR", "root@"+m.Public, cmd)
	out, err := c.CombinedOutput()
	return string(out), err
}

// sweep deletes everything carrying the bed's tag, and says what it found.
func (b *bed) sweep(ctx context.Context) (left []string) {
	tagged := linodego.NewListOptions(0, fmt.Sprintf(`{"tags": %q}`, b.tag))
	nbs, _ := b.api.ListNodeBalancers(ctx, tagged)
	for _, nb := range nbs {
		left = append(left, fmt.Sprintf("NodeBalancer %d", nb.ID))
		_ = b.api.DeleteNodeBalancer(ctx, nb.ID)
	}
	ins, _ := b.api.ListInstances(ctx, tagged)
	for _, in := range ins {
		left = append(left, "Linode "+in.Label)
		_ = b.api.DeleteInstance(ctx, in.ID)
	}
	vols, _ := b.api.ListVolumes(ctx, tagged)
	for _, v := range vols {
		left = append(left, "volume "+v.Label)
		if v.LinodeID != nil {
			_ = b.api.DetachVolume(ctx, v.ID)
			_, _ = b.api.WaitForVolumeLinodeID(ctx, v.ID, nil, 180)
		}
		_ = b.api.DeleteVolume(ctx, v.ID)
	}
	ds, _ := b.api.ListDomains(ctx, nil)
	for _, d := range ds {
		for _, tg := range d.Tags {
			if tg == b.tag {
				left = append(left, "domain "+d.Domain)
				_ = b.api.DeleteDomain(ctx, d.ID)
			}
		}
	}
	return left
}

// remaining lists what's still tagged (after a sweep: should be nothing).
func (b *bed) remaining(ctx context.Context) []string {
	tagged := linodego.NewListOptions(0, fmt.Sprintf(`{"tags": %q}`, b.tag))
	var out []string
	ins, _ := b.api.ListInstances(ctx, tagged)
	for _, in := range ins {
		out = append(out, "Linode "+in.Label)
	}
	vols, _ := b.api.ListVolumes(ctx, tagged)
	for _, v := range vols {
		out = append(out, "volume "+v.Label)
	}
	nbs, _ := b.api.ListNodeBalancers(ctx, tagged)
	for _, nb := range nbs {
		out = append(out, fmt.Sprintf("NodeBalancer %d", nb.ID))
	}
	ds, _ := b.api.ListDomains(ctx, nil)
	for _, d := range ds {
		for _, tg := range d.Tags {
			if tg == b.tag {
				out = append(out, "domain "+d.Domain)
			}
		}
	}
	return out
}

// lookup asks Linode's own name server directly (the test zone isn't delegated).
func lookup(ctx context.Context, name string) ([]string, error) {
	r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, "ns1.linode.com:53")
	}}
	return r.LookupHost(ctx, name)
}
