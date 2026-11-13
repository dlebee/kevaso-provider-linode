//go:build e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
// (a family prefix, kvt-wk- or kvt-ha-, plus a run id) as a tag and at the start of its label, and
// the sweep deletes only what has both (runOwns). The plugin labels and tags its volumes and
// NodeBalancers the same way (labelPrefix, tags). A run that died is swept by family:
// KEVASO_E2E_SWEEP=kvt-wk- go test -tags e2e -run TestSweep (familyOwns).
type bed struct {
	t      *testing.T
	api    *linodego.Client
	family string // the prefix without its dash: a second tag on everything, for finding stray runs
	tag    string
	region string
	key    string // private key file
	pub    string // authorized_keys line
	// state, from $KEVASO_E2E_STATE: a folder keeping the run (its tag, key, machines and kvo
	// home) so the next run reuses the cluster instead of making one; nothing is swept at the
	// end (TestSweep does that).
	state string
}

type savedRun struct {
	Tag, Pub string
	Machines map[string]machine
}

// restore takes over the run saved in b.state, if there is one.
func (b *bed) restore(t *testing.T) (map[string]machine, bool) {
	if b.state == "" {
		return nil, false
	}
	raw, err := os.ReadFile(filepath.Join(b.state, "run.json"))
	if err != nil {
		return nil, false
	}
	var r savedRun
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	b.tag, b.pub, b.key = r.Tag, r.Pub, filepath.Join(b.state, "id_ed25519")
	return r.Machines, true
}

// save keeps the run in b.state (with the key, readable by this user only).
func (b *bed) save(t *testing.T, ms map[string]machine) {
	if b.state == "" {
		return
	}
	_ = os.MkdirAll(b.state, 0o700)
	key, _ := os.ReadFile(b.key)
	raw, _ := json.Marshal(savedRun{Tag: b.tag, Pub: b.pub, Machines: ms})
	if err := os.WriteFile(filepath.Join(b.state, "id_ed25519"), key, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b.state, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	b.key = filepath.Join(b.state, "id_ed25519")
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
	run := make([]byte, 3) // labels are 32 characters at most: room for the plugin's

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
	if p := os.Getenv("KEVASO_E2E_PREFIX"); p != "" {
		prefix = p
	}
	if !validPrefix(prefix) {
		t.Fatalf("prefix %q: want kvt-NAME- or kevaso-e2e-", prefix)
	}
	return &bed{t: t, api: &c, family: strings.TrimSuffix(prefix, "-"), tag: prefix + hex.EncodeToString(run), region: region, key: keyFile,
		state: os.Getenv("KEVASO_E2E_STATE"),
		pub:   strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))}
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
// account holds none of them after (KEVASO_E2E_KEEP=1 keeps a failed run's; with KEVASO_E2E_STATE
// the cluster is kept for the next run, and TestSweep deletes it).
func (b *bed) cleanupAtEnd(t *testing.T) {
	t.Cleanup(func() {
		if b.state != "" {
			t.Logf("KEEPING the cluster in %s: KEVASO_E2E_SWEEP=%s- go test -tags e2e -run TestSweep deletes it", b.state, b.tag)
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
	out, err := b.sshCmd(ctx, m, cmd).CombinedOutput()
	return string(out), err
}

func (b *bed) sshCmd(ctx context.Context, m machine, cmd string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", "-i", b.key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3", "-o", "LogLevel=ERROR", "root@"+m.Public, cmd)
}

// owned lists the account's resources that owns says are the test's: Linodes, volumes,
// NodeBalancers and domains (a domain by its first label, the zone being TAG.kevaso.com).
func (b *bed) owned(ctx context.Context, owns func(label string, tags []string) bool) (ins []linodego.Instance,
	vols []linodego.Volume, nbs []linodego.NodeBalancer, ds []linodego.Domain, err error) {
	all, err := b.api.ListInstances(ctx, nil)
	if err != nil {
		return
	}
	for _, in := range all {
		if owns(in.Label, in.Tags) {
			ins = append(ins, in)
		}
	}
	allv, err := b.api.ListVolumes(ctx, nil)
	if err != nil {
		return
	}
	for _, v := range allv {
		if owns(v.Label, v.Tags) {
			vols = append(vols, v)
		}
	}
	if alln, e := b.api.ListNodeBalancers(ctx, nil); e == nil { // the token may lack the scope
		for _, nb := range alln {
			if nb.Label != nil && owns(*nb.Label, nb.Tags) {
				nbs = append(nbs, nb)
			}
		}
	}
	if alld, e := b.api.ListDomains(ctx, nil); e == nil {
		for _, d := range alld {
			if owns(strings.SplitN(d.Domain, ".", 2)[0], d.Tags) {
				ds = append(ds, d)
			}
		}
	}
	return ins, vols, nbs, ds, nil
}

func (b *bed) runOwns(label string, tags []string) bool { return runOwns(b.tag, label, tags) }

// instances are the run's Linodes.
func (b *bed) instances(ctx context.Context) []linodego.Instance {
	ins, _, _, _, _ := b.owned(ctx, b.runOwns)
	return ins
}

// sweep deletes everything of the run (runOwns: its label prefix and, where tagged, its tag),
// and says what it found.
func (b *bed) sweep(ctx context.Context) []string {
	return b.delete(ctx, b.runOwns)
}

// delete deletes what owns says is the test's; a volume still attached is detached, and deleted
// on a later round.
func (b *bed) delete(ctx context.Context, owns func(string, []string) bool) (found []string) {
	ins, vols, nbs, ds, err := b.owned(ctx, owns)
	if err != nil {
		return []string{"listing failed: " + err.Error()}
	}
	for _, nb := range nbs {
		found = append(found, "NodeBalancer "+*nb.Label)
		_ = b.api.DeleteNodeBalancer(ctx, nb.ID)
	}
	for _, in := range ins {
		found = append(found, "Linode "+in.Label)
		_ = b.api.DeleteInstance(ctx, in.ID)
	}
	for _, v := range vols {
		found = append(found, fmt.Sprintf("volume %s (%s)", v.Label, v.Status))
		if v.LinodeID != nil {
			_ = b.api.DetachVolume(ctx, v.ID)
			continue
		}
		_ = b.api.DeleteVolume(ctx, v.ID)
	}
	for _, d := range ds {
		found = append(found, "domain "+d.Domain)
		_ = b.api.DeleteDomain(ctx, d.ID)
	}
	return found
}

// remaining lists what of the run is still in the account (after a sweep: should be nothing).
func (b *bed) remaining(ctx context.Context) []string {
	ins, vols, nbs, ds, err := b.owned(ctx, b.runOwns)
	if err != nil {
		return []string{"listing failed: " + err.Error()}
	}
	var out []string
	for _, in := range ins {
		out = append(out, "Linode "+in.Label)
	}
	for _, v := range vols {
		out = append(out, "volume "+v.Label)
	}
	for _, nb := range nbs {
		out = append(out, "NodeBalancer "+*nb.Label)
	}
	for _, d := range ds {
		out = append(out, "domain "+d.Domain)
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

// TestSweep deletes what runs of a family that died left behind: every Linode, volume,
// NodeBalancer and domain labelled $KEVASO_E2E_SWEEP… (a family prefix such as kvt-wk- or
// kvt-ha-, or one run's tag plus a dash) and, where tagged, carrying the family's or a run's tag
// (familyOwns). Nothing else in the account is touched.
//
//	KEVASO_E2E_SWEEP=kvt-ha- KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -run TestSweep ./test/e2e/
func TestSweep(t *testing.T) {
	prefix, tokenFile := os.Getenv("KEVASO_E2E_SWEEP"), os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if prefix == "" || tokenFile == "" {
		t.Skip("KEVASO_E2E_SWEEP and KEVASO_LINODE_TOKEN_FILE not set")
	}
	family := prefix
	if i := strings.Index(strings.TrimPrefix(prefix, "kvt-"), "-"); strings.HasPrefix(prefix, "kvt-") && i >= 0 {
		family = prefix[:len("kvt-")+i+1] // kvt-wk-3e656e- is swept as a family of one run
	} else if strings.HasPrefix(prefix, "kevaso-e2e-") {
		family = "kevaso-e2e-"
	}
	if !validPrefix(family) {
		t.Fatalf("prefix %q: want a family prefix like kvt-ha- or kevaso-e2e-", prefix)
	}
	b := newBed(t, tokenFile, family)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	owns := func(label string, tags []string) bool {
		return strings.HasPrefix(label, prefix) && familyOwns(family, label, tags)
	}
	for i := 0; i < 20; i++ {
		found := b.delete(ctx, owns)
		if len(found) == 0 {
			t.Logf("nothing labelled %s… is left", prefix)
			return
		}
		t.Logf("round %d: %s", i, strings.Join(found, ", "))
		time.Sleep(15 * time.Second)
	}
	t.Errorf("still labelled %s… after 20 rounds", prefix)
}
