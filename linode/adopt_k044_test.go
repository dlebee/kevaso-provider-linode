package linode

import (
	"fmt"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
)

// K-044: an address Kevaso didn't reserve (another Linode's primary address: a control plane, a
// machine that isn't Kevaso's) is never shared, and letting it go never unshares it from the
// region's Linodes (sharing set up outside Kevaso stays). Only addresses Linode says are reserved
// are Kevaso's to share. Over the API shapes linodego decodes (fakeapi_test.go).
func TestOnlyReservedAddressesAreSharedOrUnshared(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(10, "ca-central", "172.105.3.63") // someone's machine: its own address
	f.addLinode(11, "ca-central", "172.105.1.1")
	f.addLinode(12, "ca-central", "172.105.2.2")
	f.shared[12] = []string{"172.105.3.63"} // sharing set up outside Kevaso
	hosts := []kplugin.HostInfo{{Name: "a", ID: "11", Region: "ca-central"}, {Name: "b", ID: "12", Region: "ca-central"}}
	writes := func() int {
		return count(f.calls, "POST /networking/ips/share") + count(f.calls, "POST /networking/ips/assign")
	}
	if _, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "x", Region: "ca-central", Address: "172.105.3.63"}); err == nil {
		t.Fatal("a Provider IP naming an address of the account")
	}
	lb := kplugin.LoadBalancer{Class: ClassSharedIP, ID: "x", Address: "172.105.3.63", Backends: hosts[:1], Hosts: hosts}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "isn't reserved") {
		t.Fatalf("sharing an address Kevaso didn't reserve: %v", err)
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassSharedIP, ID: "x", Address: "172.105.3.63"}); err != nil {
		t.Fatalf("letting go of an address not Kevaso's: %v", err)
	}
	if n := writes(); n != 0 || fmt.Sprint(f.shared[12]) != "[172.105.3.63]" {
		t.Fatalf("an address not Kevaso's was touched: %d writes, shared %v (%v)", n, f.shared, f.calls)
	}
}
