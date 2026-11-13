package linode

import (
	"encoding/json"
	"testing"

	"kevaso.com/core/sdk/kplugin"
)

// What the provider makes of a Linode's addresses, from instances in the shape GET
// /v4/linode/instances returns them. It reads only the instance's ipv4 list: the public IPv4, and
// the private one in 192.168.128.0/17 when the Linode has one. It never sets a host's internalIP or
// externalIP: the operator declares those (kvoadm --internal-ip / --external-ip), and the private
// address it reports (status.provider.privateAddress) is the host's internal IP when the operator
// declared none (core's Host.Internal). VLAN and VPC addresses aren't in the ipv4 list, so the
// provider neither reports them nor recognises a host by them.
func TestHostAddressesFromInstances(t *testing.T) {
	f, p := newFakeAPI(t)
	for _, body := range []string{
		// Public and private IPv4 (what linodetest's bed creates: private_ip true).
		`{"id": 21, "label": "w1", "region": "ca-central", "type": "g6-standard-2", "status": "running",
		  "ipv4": ["172.105.10.20", "192.168.139.44"], "ipv6": "2600:3c04::f03c:95ff:fe11:2233/128"}`,
		// Public IPv4 only.
		`{"id": 22, "label": "w2", "region": "ca-central", "type": "g6-standard-2", "status": "running",
		  "ipv4": ["172.105.10.21"], "ipv6": "2600:3c04::f03c:95ff:fe11:2234/128"}`,
		// Two public IPv4s and a private one: the private one is reported whatever the order.
		`{"id": 23, "label": "w3", "region": "us-ord", "type": "g6-standard-2", "status": "running",
		  "ipv4": ["192.168.200.9", "172.232.5.5", "172.232.5.6"], "ipv6": "2600:3c06::f03c:95ff:fe11:2235/128"}`,
	} {
		var in map[string]any
		if err := json.Unmarshal([]byte(body), &in); err != nil {
			t.Fatal(err)
		}
		f.instances = append(f.instances, in)
	}
	hosts := []kplugin.Host{
		{Name: "w1", ExternalIP: "172.105.10.20"},                       // the usual: kvoadm host add --external-ip PUBLIC
		{Name: "w1-by-private", InternalIP: "192.168.139.44"},           // recognised by its private address too
		{Name: "w2", ExternalIP: "172.105.10.21"},                       // no private address to report
		{Name: "w3", InternalIP: "10.0.4.3", ExternalIP: "172.232.5.6"}, // a declared internal IP stays the operator's
		{Name: "vlan-only", InternalIP: "10.0.4.4"},                     // a VLAN/VPC address: not in ipv4, not a Linode to the provider
	}
	got, err := p.DescribeHosts(ctx, hosts)
	if err != nil {
		t.Fatal(err)
	}
	want := []kplugin.HostInfo{
		{Name: "w1", ID: "21", Region: "ca-central", PrivateAddress: "192.168.139.44"},
		{Name: "w1-by-private", ID: "21", Region: "ca-central", PrivateAddress: "192.168.139.44"},
		{Name: "w2", ID: "22", Region: "ca-central"},
		{Name: "w3", ID: "23", Region: "us-ord", PrivateAddress: "192.168.200.9"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: got %+v, want %+v", want[i].Name, got[i], want[i])
		}
	}
}
