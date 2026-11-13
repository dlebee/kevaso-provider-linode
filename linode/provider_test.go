package linode

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/core/sdk/kplugin/kplugintest"
)

var ctx = context.Background()

func TestDescribeHosts(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(11, "ca-central", "172.105.1.1", "192.168.140.7")
	f.addLinode(12, "us-ord", "172.232.2.2")
	got, err := p.DescribeHosts(ctx, []kplugin.Host{{Name: "tor", Address: "172.105.1.1"}, {Name: "chi", Address: "172.232.2.2"}, {Name: "home", Address: "203.0.113.4"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"}) ||
		got[1] != (kplugin.HostInfo{Name: "chi", ID: "12", Region: "us-ord"}) {
		t.Fatalf("%+v", got)
	}
}

func TestVolumeLifecycle(t *testing.T) {
	f, p := newFakeAPI(t)
	tor := kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central"}
	v := kplugin.Volume{Class: ClassBlock, ID: "u/data", Name: "kvo-payments-db-data-abcde", SizeBytes: 3 << 30, Region: "ca-central"}
	st, err := p.EnsureVolume(ctx, v)
	if err != nil || st.Handle == "" || st.SizeBytes != 10<<30 {
		t.Fatalf("ensure: %+v %v (sizes round up to Linode's 10 GiB minimum)", st, err)
	}
	if again, _ := p.EnsureVolume(ctx, v); again.Handle != st.Handle {
		t.Fatalf("ensure twice made another volume: %s vs %s", again.Handle, st.Handle)
	}
	if _, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "u/data", Name: v.Name, SizeBytes: 1, Region: "us-ord"}); err == nil {
		t.Fatal("the same volume in another region")
	}
	at, err := p.AttachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: tor})
	if err != nil || at.Device != "/dev/disk/by-id/scsi-0Linode_Volume_"+v.Name {
		t.Fatalf("attach: %+v %v", at, err)
	}
	// Attached elsewhere: a conflict, never a silent move.
	chi := kplugin.HostInfo{Name: "chi", ID: "12", Region: "ca-central"}
	if _, err := p.AttachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: chi}); !errors.Is(err, kplugin.ErrConflict) {
		t.Fatalf("attach while attached elsewhere: %v", err)
	}
	if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: st.Handle}); err == nil {
		t.Fatal("deleted an attached volume")
	}
	// E032: the first detach request goes nowhere. The call doesn't block: in progress, and the
	// request is made again once DetachRetry has passed.
	f.hangDetach = 1
	err = p.DetachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: tor})
	if !errors.Is(err, kplugin.ErrInProgress) {
		t.Fatalf("hung detach: %v", err)
	}
	if err := p.DetachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: tor}); err != nil {
		t.Fatalf("detach asked again: %v", err)
	}
	if n := count(f.calls, "POST /volumes/"+st.Handle+"/detach"); n != 2 {
		t.Fatalf("detach requests: %d, want 2 (%v)", n, f.calls)
	}
	if err := p.DetachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: tor}); err != nil {
		t.Fatalf("detach when detached: %v", err)
	}
	if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: st.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: st.Handle}); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
}

func count(l []string, s string) int {
	n := 0
	for _, x := range l {
		if x == s {
			n++
		}
	}
	return n
}

func TestNodeBalancer(t *testing.T) {
	f, p := newFakeAPI(t)
	tor := kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"}
	lb := kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: "0f8c6a9e-1111-2222-3333-444455556666", Namespace: "shop", Name: "web",
		Ports:    []kplugin.Port{{Name: "http", Protocol: "TCP", Port: 80, NodePort: 30000}, {Name: "https", Protocol: "TCP", Port: 443, NodePort: 30001}},
		Backends: []kplugin.HostInfo{tor}}
	st, err := p.EnsureLoadBalancer(ctx, lb)
	if err != nil || st.Address == "" || st.SourceRanges[0] != NodeBalancerSources {
		t.Fatalf("ensure: %+v %v", st, err)
	}
	if len(f.nbs) != 1 {
		t.Fatalf("NodeBalancers: %v", f.nbs)
	}
	var nb int
	for id, x := range f.nbs {
		nb = id
		if x["label"] != NodeBalancerLabel(lb.ID) || len(x["label"].(string)) > 32 || x["region"] != "ca-central" {
			t.Fatalf("NodeBalancer %v", x)
		}
	}
	nodes := func() string {
		var out []string
		for _, c := range f.configs[nb] {
			for _, n := range c["nodes"].([]any) {
				out = append(out, fmt.Sprint(c["port"])+"→"+n.(map[string]any)["address"].(string))
			}
		}
		return strings.Join(out, " ")
	}
	if got := nodes(); got != "80→192.168.140.7:30000 443→192.168.140.7:30001" {
		t.Fatalf("backends: %s", got)
	}
	// The silo moved to another host and dropped a port: same NodeBalancer, same address.
	lb.Backends = []kplugin.HostInfo{{Name: "tor2", ID: "13", Region: "ca-central", PrivateAddress: "192.168.150.9"}}
	lb.Ports = lb.Ports[:1]
	st2, err := p.EnsureLoadBalancer(ctx, lb)
	if err != nil || st2.Address != st.Address || len(f.nbs) != 1 {
		t.Fatalf("after the move: %+v %v", st2, err)
	}
	if got := nodes(); got != "80→192.168.150.9:30000" {
		t.Fatalf("backends after the move: %s", got)
	}
	// A host without a private address can't be a backend; UDP isn't forwarded.
	bad := lb
	bad.Backends = []kplugin.HostInfo{{Name: "pub", ID: "14", Region: "ca-central"}}
	if _, err := p.EnsureLoadBalancer(ctx, bad); err == nil || !strings.Contains(err.Error(), "private IPv4") {
		t.Fatalf("no private address: %v", err)
	}
	udp := lb
	udp.Ports = []kplugin.Port{{Name: "dns", Protocol: "UDP", Port: 53, NodePort: 30002}}
	if _, err := p.EnsureLoadBalancer(ctx, udp); err == nil {
		t.Fatal("UDP through a NodeBalancer")
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassNodeBalancer, ID: lb.ID}); err != nil || len(f.nbs) != 0 {
		t.Fatalf("delete: %v %v", err, f.nbs)
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassNodeBalancer, ID: lb.ID}); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
}

func TestSharedIP(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(10, "ca-central", "172.105.3.63") // the address's owner (a spare Linode, or an extra IP)
	f.addLinode(11, "ca-central", "172.105.1.1")
	f.addLinode(12, "ca-central", "172.105.2.2")
	f.shared[12] = []string{"172.105.9.9"} // already shares something else: kept
	hosts := []kplugin.HostInfo{{Name: "a", ID: "11", Region: "ca-central"}, {Name: "b", ID: "12", Region: "ca-central"}}
	lb := kplugin.LoadBalancer{Class: ClassSharedIP, ID: "u", Address: "172.105.3.63", Backends: hosts[:1], Hosts: hosts,
		Ports: []kplugin.Port{{Name: "p2p", Protocol: "UDP", Port: 30303}}}
	st, err := p.EnsureLoadBalancer(ctx, lb)
	if err != nil || st.Address != "172.105.3.63" {
		t.Fatalf("ensure: %+v %v", st, err)
	}
	if fmt.Sprint(f.shared[11]) != "[172.105.3.63]" || fmt.Sprint(f.shared[12]) != "[172.105.9.9 172.105.3.63]" {
		t.Fatalf("shared: %v", f.shared)
	}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil || count(f.calls, "POST /networking/ips/share") != 2 {
		t.Fatalf("ensure again changed the shares: %v %v", err, f.calls)
	}
	lb.Backends = []kplugin.HostInfo{{Name: "c", ID: "20", Region: "us-ord"}}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "is in ca-central") {
		t.Fatalf("a host in another region: %v", err)
	}
	lb.Address = ""
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil {
		t.Fatal("no address")
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassSharedIP, ID: "u", Address: "172.105.3.63"}); err != nil {
		t.Fatal(err)
	}
	if len(f.shared[11]) != 0 || fmt.Sprint(f.shared[12]) != "[172.105.9.9]" {
		t.Fatalf("after delete: %v", f.shared)
	}
}

func TestDNS(t *testing.T) {
	f, p := newFakeAPI(t)
	f.domains[1] = map[string]any{"id": 1, "domain": "example.com", "type": "master"}
	f.domains[2] = map[string]any{"id": 2, "domain": "shop.example.com", "type": "master"}
	f.next = 10
	f.records[1] = []map[string]any{{"id": 5, "type": "A", "name": "www", "target": "198.51.100.1", "ttl_sec": 300}}
	r := kplugin.Record{Provider: DNSName, Name: "api.shop.example.com", Type: "A", Values: []string{"172.233.0.9"}, TTL: 120, Owner: "kevaso:c/ingress/shop/api"}
	if err := p.EnsureRecord(ctx, r); err != nil {
		t.Fatal(err)
	}
	recs := func(d int) string {
		var out []string
		for _, x := range f.records[d] {
			out = append(out, fmt.Sprintf("%v %v %v", x["type"], x["name"], x["target"]))
		}
		return strings.Join(out, "; ")
	}
	// In the most specific zone, with its owner record.
	if got := recs(2); got != "TXT _kevaso-owner.api kevaso:c/ingress/shop/api; A api 172.233.0.9" {
		t.Fatalf("shop.example.com: %s", got)
	}
	// Changing the value replaces it; a second Ensure changes nothing.
	r.Values = []string{"172.233.0.10"}
	if err := p.EnsureRecord(ctx, r); err != nil {
		t.Fatal(err)
	}
	n := len(f.calls)
	if err := p.EnsureRecord(ctx, r); err != nil || hasWrite(f.calls[n:]) {
		t.Fatalf("idempotent: %v %v", err, f.calls[n:])
	}
	if got := recs(2); got != "TXT _kevaso-owner.api kevaso:c/ingress/shop/api; A api 172.233.0.10" {
		t.Fatalf("after change: %s", got)
	}
	// Someone else's: never touched.
	other := r
	other.Owner = "kevaso:other/ingress/x/y"
	if err := p.EnsureRecord(ctx, other); !errors.Is(err, kplugin.ErrConflict) {
		t.Fatalf("another owner: %v", err)
	}
	www := kplugin.Record{Provider: DNSName, Name: "www.example.com", Type: "A", Values: []string{"172.233.0.9"}, Owner: r.Owner}
	if err := p.EnsureRecord(ctx, www); !errors.Is(err, kplugin.ErrConflict) || recs(1) != "A www 198.51.100.1" {
		t.Fatalf("a record not made by Kevaso: %v; %s", err, recs(1))
	}
	if err := p.EnsureRecord(ctx, kplugin.Record{Name: "x.example.org", Type: "A", Values: []string{"1.2.3.4"}, Owner: "o"}); err == nil {
		t.Fatal("a name in no zone")
	}
	// DNS-01: a TXT, then cleaned up with its owner record.
	txt := kplugin.Record{Provider: DNSName, Name: "_acme-challenge.shop.example.com", Type: "TXT", Values: []string{"tok"}, TTL: 60, Owner: "kevaso:c/acme"}
	if err := p.EnsureRecord(ctx, txt); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteRecord(ctx, txt); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteRecord(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got := recs(2); got != "" {
		t.Fatalf("left behind: %s", got)
	}
	if err := p.DeleteRecord(ctx, r); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
}

func hasWrite(calls []string) bool {
	for _, c := range calls {
		if !strings.HasPrefix(c, "GET ") {
			return true
		}
	}
	return false
}

// A standalone Service's load balancer: one backend per host with endpoints, each on the
// service's node port.
func TestNodeBalancerForAService(t *testing.T) {
	f, p := newFakeAPI(t)
	lb := kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: "5e1f0000-aaaa-bbbb-cccc-ddddeeeeffff", Namespace: "web3", Name: "rpc",
		Ports: []kplugin.Port{{Name: "rpc", Protocol: "TCP", Port: 8545, NodePort: 32000}},
		Backends: []kplugin.HostInfo{
			{Name: "tor1", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"},
			{Name: "tor2", ID: "12", Region: "ca-central", PrivateAddress: "192.168.140.8"}}}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, cs := range f.configs {
		for _, c := range cs {
			for _, n := range c["nodes"].([]any) {
				got = append(got, fmt.Sprint(c["port"])+"→"+n.(map[string]any)["address"].(string))
			}
		}
	}
	sort.Strings(got)
	if strings.Join(got, " ") != "8545→192.168.140.7:32000 8545→192.168.140.8:32000" {
		t.Fatalf("backends: %v", got)
	}
}

// linode-block is regional: a silo on it can move to another Linode in the region.
func TestBlockIsRegional(t *testing.T) {
	info, err := (&Provider{}).Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Storage) != 1 || info.Storage[0].Topology != kplugin.TopologyRegion {
		t.Fatalf("storage classes %+v", info.Storage)
	}
}

// A label prefix replaces "kvo-" on what the provider creates, and keeps the unique end when it
// has to cut.
func TestLabelPrefix(t *testing.T) {
	f, p := newFakeAPI(t)
	p.LabelPrefix = "kvt-wk-a1b2c3-"
	for name, want := range map[string]string{
		"kvo-e2e-db-data-abcde":        "kvt-wk-a1b2c3-e2e-db-data-abcde",
		"kvo-payments-db-data-abcde":   "kvt-wk-a1b2c3-ents-db-data-abcde",
		"kvo-payments-ledger-db-12345": "kvt-wk-a1b2c3-ts-ledger-db-12345",
	} {
		if got := p.label(name); got != want || len(got) > maxLabel {
			t.Errorf("label(%s) = %s (%d), want %s", name, got, len(got), want)
		}
	}
	st, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "u/data", Name: "kvo-e2e-db-data-abcde", SizeBytes: 1, Region: "ca-central"})
	if err != nil {
		t.Fatal(err)
	}
	if l := f.volumes[mustAtoi(t, st.Handle)]["label"]; l != "kvt-wk-a1b2c3-e2e-db-data-abcde" {
		t.Fatalf("volume label %v", l)
	}
	if again, _ := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "u/data", Name: "kvo-e2e-db-data-abcde", SizeBytes: 1, Region: "ca-central"}); again.Handle != st.Handle {
		t.Fatalf("not found again by its prefixed label: %s vs %s", again.Handle, st.Handle)
	}
	p.LabelPrefix = ""
	if got := p.label("kvo-e2e-db-data-abcde"); got != "kvo-e2e-db-data-abcde" {
		t.Fatalf("no prefix: %s", got)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Addresses apart from a load balancer (core's IP objects): a reserved IPv4 in a region, shared
// with the region's hosts while a silo holds it (assigned to its first host), unshared when let go,
// and released (the reservation deleted) when the IP is deleted. An address already in the account
// is adopted and never given up. The NodeBalancer class can't reserve; its IP keeps the balancer,
// parked with no configs while nobody holds it, deleted on release.
func TestAddresses(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(11, "ca-central", "172.105.1.1")
	f.addLinode(12, "ca-central", "172.105.2.2")
	hosts := []kplugin.HostInfo{{Name: "a", ID: "11", Region: "ca-central"}, {Name: "b", ID: "12", Region: "ca-central"}}
	st, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "ip-uid", Namespace: "web3", Name: "eth", Region: "ca-central"})
	if err != nil || st.Address == "" || st.Region != "ca-central" || !strings.HasPrefix(st.Handle, "reserved:") {
		t.Fatalf("reserve: %+v %v", st, err)
	}
	if _, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "x"}); err == nil {
		t.Fatal("reserving needs a region")
	}
	if _, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassNodeBalancer, ID: "x", Region: "ca-central"}); !errors.Is(err, kplugin.ErrUnsupported) {
		t.Fatalf("a NodeBalancer address apart from its balancer: %v", err)
	}
	// A silo on b holds it: assigned to b (nobody had it), shared with a.
	lb := kplugin.LoadBalancer{Class: ClassSharedIP, ID: "ip-uid", Address: st.Address, Backends: hosts[1:], Hosts: hosts,
		Ports: []kplugin.Port{{Name: "p2p", Protocol: "UDP", Port: 30303}}}
	if got, err := p.EnsureLoadBalancer(ctx, lb); err != nil || got.Address != st.Address {
		t.Fatalf("share: %+v %v", got, err)
	}
	if f.ips[st.Address]["linode_id"] != 12 || fmt.Sprint(f.shared[11]) != "["+st.Address+"]" || len(f.shared[12]) != 0 {
		t.Fatalf("assigned to %v, shared %v", f.ips[st.Address]["linode_id"], f.shared)
	}
	// The silo moves to a: still b's, shared with a already (nothing to change).
	lb.Backends = hosts[:1]
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil || f.ips[st.Address]["linode_id"] != 12 {
		t.Fatalf("after the move: %v %v", err, f.ips[st.Address])
	}
	// Let go, then released: unshared and the reservation deleted; twice is NotFound.
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassSharedIP, ID: "ip-uid", Address: st.Address}); err != nil || len(f.shared[11]) != 0 {
		t.Fatalf("let go: %v %v", err, f.shared)
	}
	if _, ok := f.ips[st.Address]; !ok {
		t.Fatal("letting go released the address")
	}
	ref := kplugin.AddressRef{Class: ClassSharedIP, ID: "ip-uid", Address: st.Address, Handle: st.Handle}
	if err := p.ReleaseAddress(ctx, ref); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok := f.ips[st.Address]; ok {
		t.Fatal("still reserved after release")
	}
	if err := p.ReleaseAddress(ctx, ref); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("release twice: %v", err)
	}
	// One already in the account: adopted, never given up.
	f.addLinode(10, "ca-central", "172.105.3.63")
	own, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "own", Region: "ca-central", Address: "172.105.3.63"})
	if err != nil || own.Address != "172.105.3.63" || !strings.HasPrefix(own.Handle, "adopted:") {
		t.Fatalf("adopt: %+v %v", own, err)
	}
	if _, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "own", Region: "us-ord", Address: "172.105.3.63"}); err == nil {
		t.Fatal("adopting from another region")
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassSharedIP, ID: "own", Address: own.Address, Handle: own.Handle}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.ips["172.105.3.63"]; !ok {
		t.Fatal("an adopted address was given up")
	}

	// The NodeBalancer: the IP's balancer, parked while nobody holds the IP, deleted on release.
	tor := kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"}
	nb := kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: "nb-ip-uid", Namespace: "shop", Name: "web",
		Ports: []kplugin.Port{{Name: "http", Protocol: "TCP", Port: 80, NodePort: 30000}}, Backends: []kplugin.HostInfo{tor}}
	first, err := p.EnsureLoadBalancer(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	parked, err := p.EnsureLoadBalancer(ctx, kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: nb.ID, Namespace: "shop", Name: "web"})
	if err != nil || parked.Address != first.Address || len(f.nbs) != 1 {
		t.Fatalf("park: %+v %v (want %s kept)", parked, err, first.Address)
	}
	for id := range f.nbs {
		if len(f.configs[id]) != 0 {
			t.Fatalf("a parked NodeBalancer forwards: %v", f.configs[id])
		}
	}
	again, err := p.EnsureLoadBalancer(ctx, nb) // a new holder
	if err != nil || again.Address != first.Address {
		t.Fatalf("a new holder: %+v %v", again, err)
	}
	if _, err := p.EnsureLoadBalancer(ctx, kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: "nothing"}); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("parking nothing: %v", err)
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassNodeBalancer, ID: nb.ID, Address: first.Address}); err != nil || len(f.nbs) != 0 {
		t.Fatalf("release: %v %v", err, f.nbs)
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassNodeBalancer, ID: nb.ID}); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("release twice: %v", err)
	}
}

func TestInfoSaysWhatEachClassCan(t *testing.T) {
	_, p := newFakeAPI(t)
	info, _ := p.Info(ctx)
	for _, c := range info.LoadBalancers {
		switch c.Name {
		case ClassSharedIP:
			if !c.Reserve || !c.SendsFrom || c.Follows != kplugin.FollowsRegion {
				t.Errorf("%s: %+v", c.Name, c)
			}
		case ClassNodeBalancer:
			if c.Reserve || c.SendsFrom {
				t.Errorf("%s: its address is the balancer's, on no host: %+v", c.Name, c)
			}
		}
	}
}

// The plugin protocol's promises for the address calls, over the wire (volumes have their own
// test against the fake API, above).
func TestConformanceAddresses(t *testing.T) {
	_, p := newFakeAPI(t)
	kplugintest.Conformance(t, kplugintest.Suite{Plugin: p, Region: "ca-central", Classes: []string{"none: volumes are TestVolumeLifecycle's"}})
}
