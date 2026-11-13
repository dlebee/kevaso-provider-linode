package linode

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
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
