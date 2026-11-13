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
	"sync"
	"testing"
	"time"

	"github.com/linode/linodego"
	"golang.org/x/crypto/ssh"
)

// bed is the Linode account, for the test's own machines: every resource it makes carries tag
// (PREFIX + a run id), and its Linodes are labelled tag-ROLE; sweep deletes everything with that
// tag or label prefix, and nothing else.
type bed struct {
	t      *testing.T
	api    *linodego.Client
	family string // the prefix without its dash: a second tag on everything, for finding stray runs
	tag    string
	region string
	key    string // private key file
	pub    string // authorized_keys line
}

// newBed opens the account with the token in tokenFile (read here, never printed); prefix names
// the test's resources (kevaso-e2e-, kvt-ha-).
func newBed(t *testing.T, tokenFile, prefix string) *bed {
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
	return &bed{t: t, api: &c, family: strings.TrimSuffix(prefix, "-"), tag: prefix + hex.EncodeToString(run), region: region, key: keyFile,
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
		PrivateIP: private, Tags: []string{b.tag, b.family}})
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
	b.waitSSH(ctx, m)
	// First boot runs cloud-init (which may restart sshd) and apt on its own: let both finish
	// before kvoadm logs in and installs anything.
	_, _ = b.ssh(ctx, m, "command -v cloud-init >/dev/null && timeout 300 cloud-init status --wait >/dev/null 2>&1; "+
		"systemctl stop unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null; "+
		"for i in $(seq 120); do fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1 || break; sleep 2; done")
	b.waitSSH(ctx, m)
	return m
}

func (b *bed) waitSSH(ctx context.Context, m machine) {
	b.t.Helper()
	for deadline := time.Now().Add(5 * time.Minute); ; {
		if out, err := b.ssh(ctx, m, "true"); err == nil {
			return
		} else if time.Now().After(deadline) {
			b.t.Fatalf("%s: no SSH: %v %s", m.Label, err, out)
		}
		time.Sleep(5 * time.Second)
	}
}

// createSteady is create, plus a few SSH logins in a row: a Linode whose SSH resets connections
// (seen again and again on one recycled public address) is replaced, up to twice. The replacement
// is made before the bad one is deleted (Linode hands a freed address straight out again), one
// replacement at a time, so a run of four never holds more than five Linodes.
func (b *bed) createSteady(ctx context.Context, role string) machine {
	b.t.Helper()
	m := b.create(ctx, role)
	for try := 1; ; try++ {
		bad := b.sshResets(ctx, m)
		if bad == "" || try == 3 {
			return m
		}
		b.t.Logf("%s at %s resets SSH (%s): replacing it", m.Label, m.Public, bad)
		replacing.Lock()
		next := b.create(ctx, fmt.Sprintf("%s-%d", role, try+1)) // labels are unique
		b.destroy(ctx, m)
		for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
			if _, err := b.api.GetInstance(ctx, m.ID); err != nil {
				break
			}
		}
		replacing.Unlock()
		m = next
	}
}

var replacing sync.Mutex

func (b *bed) sshResets(ctx context.Context, m machine) string {
	for i := 0; i < 8; i++ {
		if out, err := b.ssh(ctx, m, "true"); err != nil {
			return fmt.Sprintf("%v %s", err, strings.TrimSpace(out))
		}
	}
	return ""
}

// createAll makes one Linode per role at once.
func (b *bed) createAll(ctx context.Context, roles ...string) map[string]machine {
	b.t.Helper()
	type made struct {
		role string
		m    machine
	}
	ch := make(chan made, len(roles))
	for _, role := range roles {
		go func() { ch <- made{role, b.createSteady(ctx, role)} }()
	}
	ms := map[string]machine{}
	for range roles {
		x := <-ch
		ms[x.role] = x.m
	}
	if b.t.Failed() {
		b.t.FailNow()
	}
	return ms
}

// powerOff shuts a Linode down through the API (as a host lost would be) and waits until it's off.
func (b *bed) powerOff(ctx context.Context, m machine) {
	b.t.Helper()
	if err := b.api.ShutdownInstance(ctx, m.ID); err != nil {
		b.t.Fatalf("shut down %s: %v", m.Label, err)
	}
	if _, err := b.api.WaitForInstanceStatus(ctx, m.ID, linodego.InstanceOffline, 300); err != nil {
		b.t.Fatalf("%s never went off: %v", m.Label, err)
	}
}

// powerOn boots it again and waits for SSH.
func (b *bed) powerOn(ctx context.Context, m machine) {
	b.t.Helper()
	if err := b.api.BootInstance(ctx, m.ID, 0); err != nil {
		b.t.Fatalf("boot %s: %v", m.Label, err)
	}
	if _, err := b.api.WaitForInstanceStatus(ctx, m.ID, linodego.InstanceRunning, 300); err != nil {
		b.t.Fatalf("%s never ran again: %v", m.Label, err)
	}
	b.waitSSH(ctx, m)
}

// destroy deletes one of the bed's Linodes (only ever one it made: the label says so).
func (b *bed) destroy(ctx context.Context, m machine) {
	b.t.Helper()
	if !strings.HasPrefix(m.Label, b.tag+"-") {
		b.t.Fatalf("refusing to delete %s: not this run's", m.Label)
	}
	if err := b.api.DeleteInstance(ctx, m.ID); err != nil {
		b.t.Errorf("delete %s: %v", m.Label, err)
	}
}

// cleanupAtEnd sweeps the run's resources when the test ends, failed or not, and checks the
// account holds none of them after (KEVASO_E2E_KEEP=1 keeps a failed run's).
func (b *bed) cleanupAtEnd(t *testing.T) {
	t.Cleanup(func() {
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
				t.Logf("nothing tagged or labelled %s is left in the account", b.tag)
				return
			}
			time.Sleep(15 * time.Second)
			b.sweep(ctx)
		}
		t.Errorf("still tagged %s after cleanup: %v", b.tag, b.remaining(ctx))
	})
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
	for _, in := range b.instances(ctx) {
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

// instances are the run's Linodes: tagged with its tag, or labelled with its prefix.
func (b *bed) instances(ctx context.Context) []linodego.Instance {
	return instancesOf(ctx, b.api, b.tag)
}

// instancesOf lists the Linodes tagged prefix or labelled prefix-… (a whole family when prefix is
// one, e.g. kvt-ha-).
func instancesOf(ctx context.Context, api *linodego.Client, prefix string) []linodego.Instance {
	all, _ := api.ListInstances(ctx, nil)
	var out []linodego.Instance
	for _, in := range all {
		mine := strings.HasPrefix(in.Label, prefix)
		for _, tg := range in.Tags {
			mine = mine || tg == prefix
		}
		if mine {
			out = append(out, in)
		}
	}
	return out
}

// remaining lists what's still tagged (after a sweep: should be nothing).
func (b *bed) remaining(ctx context.Context) []string {
	tagged := linodego.NewListOptions(0, fmt.Sprintf(`{"tags": %q}`, b.tag))
	var out []string
	for _, in := range b.instances(ctx) {
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
