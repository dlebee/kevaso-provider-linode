package linode

import (
	"fmt"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
)

// K-044: an IP object could name any IPv4 of the account (spec.address), another Linode's primary
// address included (a control plane, a machine that isn't Kevaso's): the provider shared it with
// every host in the region, and letting it go unshared it from every Linode there, sharing set up
// outside Kevaso too. Linode's IP addresses carry no tags (the API's IP object has none: see
// linodego's InstanceIP), so an address is tagged for Kevaso by being listed in the provider's
// config ("adopt"). Now only a listed address is adopted, and an address is shared or unshared only
// if the provider reserved it (Linode says "reserved") or it is listed; any other is left alone.
// Over the API shapes linodego decodes (fakeapi_test.go).
func TestOnlyAddressesTaggedForKevasoAreAdoptedOrUnshared(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(10, "ca-central", "172.105.3.63") // someone's machine: its own address
	f.addLinode(11, "ca-central", "172.105.1.1")
	f.addLinode(12, "ca-central", "172.105.2.2")
	f.shared[12] = []string{"172.105.3.63"} // sharing set up outside Kevaso
	hosts := []kplugin.HostInfo{{Name: "a", ID: "11", Region: "ca-central"}, {Name: "b", ID: "12", Region: "ca-central"}}
	writes := func() int {
		return count(f.calls, "POST /networking/ips/share") + count(f.calls, "POST /networking/ips/assign")
	}

	// Not listed: refused, saying how to tag it; nothing shared, assigned or unshared.
	_, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "x", Region: "ca-central", Address: "172.105.3.63"})
	if err == nil || !strings.Contains(err.Error(), "isn't tagged for Kevaso") || !strings.Contains(err.Error(), `"adopt"`) {
		t.Fatalf("adopting an address not tagged for Kevaso: %v", err)
	}
	lb := kplugin.LoadBalancer{Class: ClassSharedIP, ID: "x", Address: "172.105.3.63", Backends: hosts[:1], Hosts: hosts}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "isn't tagged for Kevaso") {
		t.Fatalf("sharing an address not tagged for Kevaso: %v", err)
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassSharedIP, ID: "x", Address: "172.105.3.63"}); err != nil {
		t.Fatalf("letting go of an address not Kevaso's: %v", err)
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassSharedIP, ID: "x", Address: "172.105.3.63", Handle: "adopted:172.105.3.63"}); err != nil {
		t.Fatalf("releasing an address not Kevaso's: %v", err)
	}
	if n := writes(); n != 0 || fmt.Sprint(f.shared[12]) != "[172.105.3.63]" {
		t.Fatalf("an untagged address was touched: %d writes, shared %v (%v)", n, f.shared, f.calls)
	}

	// Listed in the config: adopted, shared, unshared on release, never given up.
	p.Adopt = []string{"172.105.3.63"}
	own, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "x", Region: "ca-central", Address: "172.105.3.63"})
	if err != nil || own.Handle != "adopted:172.105.3.63" {
		t.Fatalf("adopting a tagged address: %+v %v", own, err)
	}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil || fmt.Sprint(f.shared[11]) != "[172.105.3.63]" {
		t.Fatalf("sharing a tagged address: %v %v", err, f.shared)
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassSharedIP, ID: "x", Address: own.Address, Handle: own.Handle}); err != nil {
		t.Fatal(err)
	}
	if len(f.shared[11]) != 0 || len(f.shared[12]) != 0 {
		t.Fatalf("a tagged address not unshared on release: %v", f.shared)
	}
	if _, ok := f.ips["172.105.3.63"]; !ok {
		t.Fatal("an adopted address was given up")
	}
}
