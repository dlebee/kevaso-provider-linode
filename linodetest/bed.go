package linodetest

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linode/linodego"
	"golang.org/x/crypto/ssh"
)

// Bed is the Linode account, for the test's own machines: every resource it makes carries tag
// (a family prefix, kvt-wk- or kvt-ha-, plus a run id) as a tag and at the start of its label, and
// the sweep deletes only what has both (runOwns). The plugin labels and tags its volumes and
// NodeBalancers the same way (labelPrefix, tags). A run that died is swept by family:
// KEVASO_E2E_SWEEP=kvt-wk- go test -tags e2e -run TestSweep (familyOwns).
type Bed struct {
	T      *testing.T
	API    *linodego.Client
	Family string // the prefix without its dash: a second tag on everything, for finding stray runs
	Tag    string
	Region string
	Key    string // private key file
	Pub    string // authorized_keys line
	// state, from $KEVASO_E2E_STATE: a folder keeping the run (its tag, key, machines and kvo
	// home) so the next run reuses the cluster instead of making one; nothing is swept at the
	// end (TestSweep does that).
	State string
}

type savedRun struct {
	Tag, Pub string
	Machines map[string]Machine
}

// Restore takes over the run saved in b.State, if there is one.
func (b *Bed) Restore(t *testing.T) (map[string]Machine, bool) {
	if b.State == "" {
		return nil, false
	}
	raw, err := os.ReadFile(filepath.Join(b.State, "run.json"))
	if err != nil {
		return nil, false
	}
	var r savedRun
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	b.Tag, b.Pub, b.Key = r.Tag, r.Pub, filepath.Join(b.State, "id_ed25519")
	return r.Machines, true
}

// Save keeps the run in b.State (with the key, readable by this user only).
func (b *Bed) Save(t *testing.T, ms map[string]Machine) {
	if b.State == "" {
		return
	}
	_ = os.MkdirAll(b.State, 0o700)
	key, _ := os.ReadFile(b.Key)
	raw, _ := json.Marshal(savedRun{Tag: b.Tag, Pub: b.Pub, Machines: ms})
	if err := os.WriteFile(filepath.Join(b.State, "id_ed25519"), key, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b.State, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	b.Key = filepath.Join(b.State, "id_ed25519")
}

// NewBed opens the account with the token in tokenFile (read here, never printed); prefix names
// the test's resources (kevaso-e2e-, kvt-ha-).
func NewBed(t *testing.T, tokenFile, prefix string) *Bed {
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
	return &Bed{T: t, API: &c, Family: strings.TrimSuffix(prefix, "-"), Tag: prefix + hex.EncodeToString(run), Region: region, Key: keyFile,
		State: os.Getenv("KEVASO_E2E_STATE"),
		Pub:   strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))}
}

// Machine is one Linode of the test.
type Machine struct {
	ID              int
	Label           string
	Public, Private string
}

// Create makes a nanode (Ubuntu 24.04, with a private IPv4) and waits until SSH answers.
func (b *Bed) Create(ctx context.Context, role string) Machine {
	b.T.Helper()
	pw := make([]byte, 24)
	_, _ = rand.Read(pw)
	private := true
	label := b.Tag + "-" + role
	in, err := b.createInstance(ctx, linodego.InstanceCreateOptions{Region: b.Region, Type: "g6-nanode-1", Label: label,
		Image: "linode/ubuntu24.04", RootPass: hex.EncodeToString(pw) + "Aa1!", AuthorizedKeys: []string{b.Pub},
		PrivateIP: private, Tags: []string{b.Tag, b.Family}})
	if err != nil {
		b.T.Fatalf("create %s: %v", role, err)
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
		b.T.Fatalf("%s never ran: %v", role, err)
	}
	b.WaitSSH(ctx, m)
	// First boot runs cloud-init (which may restart sshd) and apt on its own: let both finish
	// before kvoadm logs in and installs anything.
	_, _ = b.SSH(ctx, m, "command -v cloud-init >/dev/null && timeout 300 cloud-init status --wait >/dev/null 2>&1; "+
		"systemctl stop unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null; "+
		"for i in $(seq 120); do fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1 || break; sleep 2; done")
	b.WaitSSH(ctx, m)
	return m
}

// createInstance creates a Linode, and is safe to try again: when the answer is lost (the
// connection reset after the POST was sent) the Linode may exist all the same, so before each new
// try it looks for one with the label (unique to the run) and takes that one instead.
func (b *Bed) createInstance(ctx context.Context, o linodego.InstanceCreateOptions) (*linodego.Instance, error) {
	var last error
	for try := 1; try <= 4; try++ {
		in, err := b.API.CreateInstance(ctx, o)
		if err == nil {
			return in, nil
		}
		last = err
		var le *linodego.Error
		if errors.As(err, &le) && le.Code >= 400 && le.Code < 500 {
			return nil, err // refused: trying again changes nothing
		}
		b.T.Logf("create %s (try %d): %v; looking for it before trying again", o.Label, try, err)
		for look := 0; look < 3; look++ {
			time.Sleep(10 * time.Second)
			f, _ := json.Marshal(map[string]string{"label": o.Label})
			if got, err := b.API.ListInstances(ctx, linodego.NewListOptions(0, string(f))); err == nil {
				if len(got) == 1 {
					b.T.Logf("%s was made all the same (%d): taking it", o.Label, got[0].ID)
					return &got[0], nil
				}
				break // asked, and it isn't there
			}
		}
	}
	return nil, last
}

// WaitSSH waits (up to 5 minutes) until m answers SSH.
func (b *Bed) WaitSSH(ctx context.Context, m Machine) {
	b.T.Helper()
	for deadline := time.Now().Add(5 * time.Minute); ; {
		if out, err := b.SSH(ctx, m, "true"); err == nil {
			return
		} else if time.Now().After(deadline) {
			b.T.Fatalf("%s: no SSH: %v %s", m.Label, err, out)
		}
		time.Sleep(5 * time.Second)
	}
}

// CreateSteady is Create, plus a few SSH logins in a row: a Linode whose SSH resets connections
// (seen again and again on one recycled public address) is replaced, up to twice. The replacement
// is made before the bad one is deleted (Linode hands a freed address straight out again), one
// replacement at a time, so a run of four never holds more than five Linodes.
func (b *Bed) CreateSteady(ctx context.Context, role string) Machine {
	b.T.Helper()
	m := b.Create(ctx, role)
	for try := 1; ; try++ {
		bad := b.sshResets(ctx, m)
		if bad == "" || try == 3 {
			return m
		}
		b.T.Logf("%s at %s resets SSH (%s): replacing it", m.Label, m.Public, bad)
		replacing.Lock()
		next := b.Create(ctx, fmt.Sprintf("%s-%d", role, try+1)) // labels are unique
		b.Destroy(ctx, m)
		for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
			if _, err := b.API.GetInstance(ctx, m.ID); err != nil {
				break
			}
		}
		replacing.Unlock()
		m = next
	}
}

var replacing sync.Mutex

func (b *Bed) sshResets(ctx context.Context, m Machine) string {
	for i := 0; i < 8; i++ {
		if out, err := b.SSH(ctx, m, "true"); err != nil {
			return fmt.Sprintf("%v %s", err, strings.TrimSpace(out))
		}
	}
	return ""
}

// CreateAll makes one Linode per role at once.
func (b *Bed) CreateAll(ctx context.Context, roles ...string) map[string]Machine {
	b.T.Helper()
	type made struct {
		role string
		m    Machine
	}
	ch := make(chan made, len(roles))
	for _, role := range roles {
		go func() {
			sent := false
			// A failed create ends this goroutine with t.Fatalf (runtime.Goexit): still answer,
			// or CreateAll waits forever and the run's cleanup never runs.
			defer func() {
				if !sent {
					ch <- made{role: role}
				}
			}()
			m := b.CreateSteady(ctx, role)
			sent = true
			ch <- made{role, m}
		}()
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

// PowerOff shuts a Linode down through the API (as a host lost would be) and waits until it's off.
func (b *Bed) PowerOff(ctx context.Context, m Machine) {
	b.T.Helper()
	if err := b.API.ShutdownInstance(ctx, m.ID); err != nil {
		b.T.Fatalf("shut down %s: %v", m.Label, err)
	}
	if _, err := b.API.WaitForInstanceStatus(ctx, m.ID, linodego.InstanceOffline, 300); err != nil {
		b.T.Fatalf("%s never went off: %v", m.Label, err)
	}
}

// PowerOn boots it again and waits for SSH.
func (b *Bed) PowerOn(ctx context.Context, m Machine) {
	b.T.Helper()
	if err := b.API.BootInstance(ctx, m.ID, 0); err != nil {
		b.T.Fatalf("boot %s: %v", m.Label, err)
	}
	if _, err := b.API.WaitForInstanceStatus(ctx, m.ID, linodego.InstanceRunning, 300); err != nil {
		b.T.Fatalf("%s never ran again: %v", m.Label, err)
	}
	b.WaitSSH(ctx, m)
}

// Destroy deletes one of the bed's Linodes (only ever one it made: the label says so).
func (b *Bed) Destroy(ctx context.Context, m Machine) {
	b.T.Helper()
	if !strings.HasPrefix(m.Label, b.Tag+"-") {
		b.T.Fatalf("refusing to delete %s: not this run's", m.Label)
	}
	if err := b.API.DeleteInstance(ctx, m.ID); err != nil {
		b.T.Errorf("delete %s: %v", m.Label, err)
	}
}

// CleanupAtEnd sweeps the run's resources when the test ends, failed or not, and checks the
// account holds none of them after (KEVASO_E2E_KEEP=1 keeps a failed run's; with KEVASO_E2E_STATE
// the cluster is kept for the next run, and TestSweep deletes it).
func (b *Bed) CleanupAtEnd(t *testing.T) {
	t.Cleanup(func() {
		if b.State != "" {
			t.Logf("KEEPING the cluster in %s: KEVASO_E2E_SWEEP=%s- go test -tags e2e -run TestSweep deletes it", b.State, b.Tag)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		if os.Getenv("KEVASO_E2E_KEEP") != "" && t.Failed() {
			t.Logf("KEEPING everything tagged %s", b.Tag)
			return
		}
		swept := b.Sweep(ctx)
		t.Logf("swept: %s", strings.Join(swept, ", "))
		for i := 0; i < 20; i++ {
			left := b.Remaining(ctx)
			if len(left) == 0 {
				t.Logf("nothing tagged or labelled %s is left in the account", b.Tag)
				return
			}
			time.Sleep(15 * time.Second)
			b.Sweep(ctx)
		}
		t.Errorf("still tagged %s after cleanup: %v", b.Tag, b.Remaining(ctx))
	})
}

// SSH runs cmd as root on m, and returns what it printed.
func (b *Bed) SSH(ctx context.Context, m Machine, cmd string) (string, error) {
	out, err := b.SSHCmd(ctx, m, cmd).CombinedOutput()
	return string(out), err
}

// SSHIn is SSH with stdin: how a secret reaches a machine, never on a command line.
func (b *Bed) SSHIn(ctx context.Context, m Machine, stdin, cmd string) (string, error) {
	c := b.SSHCmd(ctx, m, cmd)
	c.Stdin = strings.NewReader(stdin)
	out, err := c.CombinedOutput()
	return string(out), err
}

// SSHCmd is the ssh command that runs cmd as root on m.

func (b *Bed) SSHCmd(ctx context.Context, m Machine, cmd string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", "-i", b.Key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3", "-o", "LogLevel=ERROR", "root@"+m.Public, cmd)
}

// Resources are the account's resources a test can make, by kind.
type Resources struct {
	Instances     []linodego.Instance
	Volumes       []linodego.Volume
	NodeBalancers []linodego.NodeBalancer
	Domains       []linodego.Domain
	Firewalls     []linodego.Firewall
	VPCs          []linodego.VPC
}

// Lines says what r holds, one "kind label" per resource.
func (r Resources) Lines() []string {
	var out []string
	for _, in := range r.Instances {
		out = append(out, "Linode "+in.Label)
	}
	for _, v := range r.Volumes {
		out = append(out, "volume "+v.Label)
	}
	for _, nb := range r.NodeBalancers {
		out = append(out, "NodeBalancer "+*nb.Label)
	}
	for _, d := range r.Domains {
		out = append(out, "domain "+d.Domain)
	}
	for _, f := range r.Firewalls {
		out = append(out, "firewall "+f.Label)
	}
	for _, v := range r.VPCs {
		out = append(out, "VPC "+v.Label)
	}
	return out
}

// Owned lists the account's resources that owns says are the test's: Linodes, volumes,
// NodeBalancers, domains (a domain by its first label, the zone being TAG.kevaso.com),
// firewalls and VPCs.
func (b *Bed) Owned(ctx context.Context, owns func(label string, tags []string) bool) (ins []linodego.Instance,
	vols []linodego.Volume, nbs []linodego.NodeBalancer, ds []linodego.Domain, err error) {
	r, err := b.OwnedAll(ctx, owns)
	return r.Instances, r.Volumes, r.NodeBalancers, r.Domains, err
}

// OwnedAll is Owned with every kind.
func (b *Bed) OwnedAll(ctx context.Context, owns func(label string, tags []string) bool) (r Resources, err error) {
	var (
		ins  []linodego.Instance
		vols []linodego.Volume
		nbs  []linodego.NodeBalancer
		ds   []linodego.Domain
	)
	defer func() { r.Instances, r.Volumes, r.NodeBalancers, r.Domains = ins, vols, nbs, ds }()
	all, err := b.API.ListInstances(ctx, nil)
	if err != nil {
		return
	}
	for _, in := range all {
		if owns(in.Label, in.Tags) {
			ins = append(ins, in)
		}
	}
	allv, err := b.API.ListVolumes(ctx, nil)
	if err != nil {
		return
	}
	for _, v := range allv {
		if owns(v.Label, v.Tags) {
			vols = append(vols, v)
		}
	}
	if alln, e := b.API.ListNodeBalancers(ctx, nil); e == nil { // the token may lack the scope
		for _, nb := range alln {
			if nb.Label != nil && owns(*nb.Label, nb.Tags) {
				nbs = append(nbs, nb)
			}
		}
	}
	if alld, e := b.API.ListDomains(ctx, nil); e == nil {
		for _, d := range alld {
			if owns(strings.SplitN(d.Domain, ".", 2)[0], d.Tags) {
				ds = append(ds, d)
			}
		}
	}
	if fws, e := b.API.ListFirewalls(ctx, nil); e == nil { // the token may lack the scope
		for _, f := range fws {
			if owns(f.Label, f.Tags) {
				r.Firewalls = append(r.Firewalls, f)
			}
		}
	}
	if vpcs, e := b.API.ListVPCs(ctx, nil); e == nil { // VPCs have no tags: by label only
		for _, v := range vpcs {
			if owns(v.Label, nil) {
				r.VPCs = append(r.VPCs, v)
			}
		}
	}
	return r, nil
}

// RunOwns says whether a resource (its label and tags) is the run's (runOwns).
func (b *Bed) RunOwns(label string, tags []string) bool { return runOwns(b.Tag, label, tags) }

// Instances are the run's Linodes.
func (b *Bed) Instances(ctx context.Context) []linodego.Instance {
	ins, _, _, _, _ := b.Owned(ctx, b.RunOwns)
	return ins
}

// Sweep deletes everything of the run (runOwns: its label prefix and, where tagged, its tag),
// and says what it found.
func (b *Bed) Sweep(ctx context.Context) []string {
	return b.Delete(ctx, b.RunOwns)
}

// Delete deletes what owns says is the test's; a volume still attached is detached, and deleted
// on a later round.
func (b *Bed) Delete(ctx context.Context, owns func(string, []string) bool) (found []string) {
	r, err := b.OwnedAll(ctx, owns)
	if err != nil {
		return []string{"listing failed: " + err.Error()}
	}
	ins, vols, nbs, ds := r.Instances, r.Volumes, r.NodeBalancers, r.Domains
	for _, nb := range nbs {
		found = append(found, "NodeBalancer "+*nb.Label)
		_ = b.API.DeleteNodeBalancer(ctx, nb.ID)
	}
	for _, in := range ins {
		found = append(found, "Linode "+in.Label)
		_ = b.API.DeleteInstance(ctx, in.ID)
	}
	for _, v := range vols {
		found = append(found, fmt.Sprintf("volume %s (%s)", v.Label, v.Status))
		if v.LinodeID != nil {
			_ = b.API.DetachVolume(ctx, v.ID)
			continue
		}
		_ = b.API.DeleteVolume(ctx, v.ID)
	}
	for _, d := range ds {
		found = append(found, "domain "+d.Domain)
		_ = b.API.DeleteDomain(ctx, d.ID)
	}
	for _, f := range r.Firewalls {
		found = append(found, "firewall "+f.Label)
		_ = b.API.DeleteFirewall(ctx, f.ID) // its devices go with it
	}
	if len(ins) == 0 { // a VPC goes once no Linode of the run is on its subnets
		for _, v := range r.VPCs {
			found = append(found, "VPC "+v.Label)
			_ = b.API.DeleteVPC(ctx, v.ID)
		}
	}
	return found
}

// Remaining lists what of the run is still in the account (after a sweep: should be nothing).
func (b *Bed) Remaining(ctx context.Context) []string {
	r, err := b.OwnedAll(ctx, b.RunOwns)
	if err != nil {
		return []string{"listing failed: " + err.Error()}
	}
	return r.Lines()
}

// Leftovers lists, deleting nothing, every resource in the account whose label starts with kvt-
// or kevaso-e2e- (any family, any run), and how many reserved IPv4 addresses the account holds
// (they carry no label: a test that reserved one releases it itself).
func (b *Bed) Leftovers(ctx context.Context) ([]string, int, error) {
	r, err := b.OwnedAll(ctx, func(label string, _ []string) bool {
		return strings.HasPrefix(label, "kvt-") || strings.HasPrefix(label, "kevaso-e2e-")
	})
	if err != nil {
		return nil, 0, err
	}
	reserved, err := b.API.ListReservedIPAddresses(ctx, nil)
	if err != nil {
		return r.Lines(), -1, nil // the token may lack the scope
	}
	return r.Lines(), len(reserved), nil
}

// Extras lists, deleting nothing, the account's IPv6 ranges and its Linodes' additional public
// IPv4s (any beyond a Linode's own first one: assigned or shared to it), neither of which carries
// a label. Each line names the Linodes it is on. Ours are those on a kvt-/kevaso-e2e- Linode, or
// on no Linode at all (a range or reservation left behind); the rest belong to the account's other
// Linodes and are only counted in others.
func (b *Bed) Extras(ctx context.Context) (ours []string, others int, err error) {
	ins, err := b.API.ListInstances(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	label := map[int]string{}
	for _, in := range ins {
		label[in.ID] = in.Label
	}
	test := func(id int) bool {
		l, ok := label[id]
		return !ok || strings.HasPrefix(l, "kvt-") || strings.HasPrefix(l, "kevaso-e2e-")
	}
	named := func(ids []int) string {
		var ns []string
		for _, id := range ids {
			ns = append(ns, fmt.Sprintf("%d (%s)", id, cmp.Or(label[id], "gone")))
		}
		return strings.Join(ns, ", ")
	}
	ranges, err := b.API.ListIPv6Ranges(ctx, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("IPv6 ranges: %w", err)
	}
	for _, r := range ranges {
		got, err := b.API.GetIPv6Range(ctx, r.Range)
		if err != nil {
			return nil, 0, fmt.Errorf("IPv6 range %s: %w", r.Range, err)
		}
		if len(got.Linodes) > 0 && !slices.ContainsFunc(got.Linodes, test) {
			others++
			continue
		}
		ours = append(ours, fmt.Sprintf("IPv6 range %s/%d in %s on Linodes [%s]", r.Range, r.Prefix, r.Region, named(got.Linodes)))
	}
	ips, err := b.API.ListIPAddresses(ctx, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("IP addresses: %w", err)
	}
	public := map[int][]string{} // a Linode's public IPv4s: its own and any more
	var order []int
	for _, ip := range ips {
		if ip.Type != linodego.IPTypeIPv4 || !ip.Public || ip.LinodeID == 0 {
			continue // reserved and unassigned: counted by Leftovers
		}
		if public[ip.LinodeID] == nil {
			order = append(order, ip.LinodeID)
		}
		public[ip.LinodeID] = append(public[ip.LinodeID], ip.Address)
	}
	for _, id := range order {
		switch as := public[id]; {
		case len(as) < 2:
		case !test(id):
			others += len(as) - 1
		default:
			ours = append(ours, fmt.Sprintf("Linode %s has %d public IPv4s (one is its own): %s", named([]int{id}), len(as), strings.Join(as, ", ")))
		}
	}
	return ours, others, nil
}

// Lookup asks Linode's own name server directly (the test zone isn't delegated).
func Lookup(ctx context.Context, name string) ([]string, error) {
	r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, "ns1.linode.com:53")
	}}
	return r.LookupHost(ctx, name)
}

// SweepFamily deletes what runs of a family that died left behind: every Linode, volume,
// NodeBalancer and domain labelled prefix… (a family prefix such as kvt-wk- or kvt-ha-, or one
// run's tag plus a dash) and, where tagged, carrying the family's or a run's tag (familyOwns).
// Nothing else in the account is touched. Each provider's TestSweep calls it:
//
//	KEVASO_E2E_SWEEP=kvt-ha- KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -run TestSweep ./test/e2e/
func SweepFamily(t *testing.T, prefix, tokenFile string) {
	family := prefix
	if i := strings.Index(strings.TrimPrefix(prefix, "kvt-"), "-"); strings.HasPrefix(prefix, "kvt-") && i >= 0 {
		family = prefix[:len("kvt-")+i+1] // kvt-wk-3e656e- is swept as a family of one run
	} else if strings.HasPrefix(prefix, "kevaso-e2e-") {
		family = "kevaso-e2e-"
	}
	if !validPrefix(family) {
		t.Fatalf("prefix %q: want a family prefix like kvt-ha- or kevaso-e2e-", prefix)
	}
	b := NewBed(t, tokenFile, family)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	owns := func(label string, tags []string) bool {
		return strings.HasPrefix(label, prefix) && familyOwns(family, label, tags)
	}
	for i := 0; i < 20; i++ {
		found := b.Delete(ctx, owns)
		if len(found) == 0 {
			t.Logf("nothing labelled %s… is left", prefix)
			return
		}
		t.Logf("round %d: %s", i, strings.Join(found, ", "))
		time.Sleep(15 * time.Second)
	}
	t.Errorf("still labelled %s… after 20 rounds", prefix)
}
