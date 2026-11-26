package linode

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
)

var (
	torA = kplugin.HostInfo{Name: "tor-a", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"}
	torB = kplugin.HostInfo{Name: "tor-b", ID: "12", Region: "ca-central", PrivateAddress: "192.168.140.8"}
	ord  = kplugin.HostInfo{Name: "ord-a", ID: "21", Region: "us-ord", PrivateAddress: "192.168.150.7"}
)

func service(id, ns, name string, ann map[string]string, ports ...int) kplugin.LoadBalancer {
	lb := kplugin.LoadBalancer{Class: ClassNodeBalancer, ID: id, Namespace: ns, Name: name, Backends: []kplugin.HostInfo{torA}, Annotations: ann}
	for i, p := range ports {
		lb.Ports = append(lb.Ports, kplugin.Port{Name: fmt.Sprint("p", p), Protocol: "TCP", Port: p, NodePort: 30000 + i})
	}
	return lb
}

// configsOf lists one NodeBalancer's configs as "PORT→ADDR,ADDR".
func configsOf(f *fakeAPI, label string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id, nb := range f.nbs {
		if nb["label"] != label {
			continue
		}
		for _, c := range f.configs[id] {
			var nodes []string
			for _, n := range c["nodes"].([]any) {
				nodes = append(nodes, n.(map[string]any)["address"].(string))
			}
			out = append(out, fmt.Sprint(c["port"])+"→"+strings.Join(nodes, ","))
		}
	}
	sort.Strings(out)
	return out
}

// One NodeBalancer per service by default: two services get two, each with its own address;
// deleting one removes only its own.
func TestNodeBalancerPerService(t *testing.T) {
	f, p := newFakeAPI(t)
	web, api := service("uid-1/web", "shop", "web", nil, 443), service("uid-1/api", "shop", "api", nil, 443)
	a, err := p.EnsureLoadBalancer(ctx, web)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.EnsureLoadBalancer(ctx, api)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.nbs) != 2 || a.Address == b.Address {
		t.Fatalf("two services, one port each: %v (%s, %s)", f.nbs, a.Address, b.Address)
	}
	if got := configsOf(f, NodeBalancerLabel(web.ID)); fmt.Sprint(got) != "[443→192.168.140.7:30000]" {
		t.Fatalf("web's: %v", got)
	}
	again, err := p.EnsureLoadBalancer(ctx, web)
	if err != nil || again.Address != a.Address || len(f.nbs) != 2 {
		t.Fatalf("ensured again: %+v %v", again, err)
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassNodeBalancer, ID: web.ID}); err != nil || len(f.nbs) != 1 {
		t.Fatalf("web gone: %v %v", err, f.nbs)
	}
	if got := configsOf(f, NodeBalancerLabel(api.ID)); len(got) != 1 {
		t.Fatalf("api's after web went: %v", got)
	}
}

// Services of a namespace naming the same nodebalancer share it, each on its own ports: created
// with the first, a port taken is refused, removed with the last. Another namespace's "edge" is
// another NodeBalancer. Moving off it (the annotation removed) leaves it.
func TestSharedNodeBalancerByName(t *testing.T) {
	f, p := newFakeAPI(t)
	edge := map[string]string{"linode.kevaso.com/nodebalancer": "edge"}
	web := service("uid-1/web", "shop", "web", edge, 80, 443)
	api := service("uid-2/api", "shop", "api", map[string]string{"linode.kevaso.com/nodebalancer": "edge", "linode.kevaso.com/proxy-protocol": "v2"}, 8443)
	api.Backends = []kplugin.HostInfo{torB}
	a, err := p.EnsureLoadBalancer(ctx, web)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.EnsureLoadBalancer(ctx, api)
	if err != nil || b.Address != a.Address || len(f.nbs) != 1 {
		t.Fatalf("shared: %+v %v %v", b, err, f.nbs)
	}
	label := SharedNodeBalancerLabel("shop", "edge")
	if len(label) > maxLabel {
		t.Fatalf("label %s too long", label)
	}
	if got := fmt.Sprint(configsOf(f, label)); got != "[443→192.168.140.7:30001 80→192.168.140.7:30000 8443→192.168.140.8:30000]" {
		t.Fatalf("configs: %s", got)
	}
	for _, cs := range f.configs {
		for _, c := range cs {
			if want := map[bool]string{true: "v2", false: "none"}[fmt.Sprint(c["port"]) == "8443"]; c["proxy_protocol"] != want {
				t.Errorf("port %v proxy protocol %v, want %s (per service)", c["port"], c["proxy_protocol"], want)
			}
		}
	}
	// A third service on port 443 of edge: refused, nothing changed.
	clash := service("uid-3/admin", "shop", "admin", edge, 443)
	if _, err := p.EnsureLoadBalancer(ctx, clash); !errors.Is(err, kplugin.ErrConflict) || !strings.Contains(err.Error(), "port 443") {
		t.Fatalf("a port taken: %v", err)
	}
	// Web drops port 80: its config goes, the others stay.
	web.Ports = web.Ports[1:]
	if _, err := p.EnsureLoadBalancer(ctx, web); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(configsOf(f, label)); got != "[443→192.168.140.7:30001 8443→192.168.140.8:30000]" {
		t.Fatalf("after web dropped 80: %s", got)
	}
	// The same name in another namespace is another NodeBalancer.
	other := service("uid-9/web", "blog", "web", edge, 443)
	if o, err := p.EnsureLoadBalancer(ctx, other); err != nil || o.Address == a.Address || len(f.nbs) != 2 {
		t.Fatalf("another namespace's edge: %+v %v", o, err)
	}
	// Api leaves edge (its annotation removed): on its own NodeBalancer, off edge.
	api.Annotations = nil
	if _, err := p.EnsureLoadBalancer(ctx, api); err != nil || len(f.nbs) != 3 {
		t.Fatalf("api moved: %v %v", err, f.nbs)
	}
	if got := fmt.Sprint(configsOf(f, label)); got != "[443→192.168.140.7:30001]" {
		t.Fatalf("edge after api left: %s", got)
	}
	// Web, the last on edge, goes: edge goes.
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassNodeBalancer, ID: web.ID}); err != nil {
		t.Fatal(err)
	}
	for _, nb := range f.nbs {
		if nb["label"] == label {
			t.Fatalf("edge kept with no service: %v", nb)
		}
	}
	if err := p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ClassNodeBalancer, ID: web.ID}); !errors.Is(err, kplugin.ErrNotFound) {
		t.Fatalf("web gone twice: %v", err)
	}
}

// The annotations: validated against the declaration (a misspelling says what was meant), read
// under whichever name the plugin is installed as, and turned into the NodeBalancer's settings.
func TestNodeBalancerAnnotations(t *testing.T) {
	f, p := newFakeAPI(t)
	lb := service("uid-1/web", "shop", "web", map[string]string{"linode.kevaso.com/nodebalancr": "edge"}, 443)
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "did you mean linode.kevaso.com/nodebalancer?") {
		t.Fatalf("a misspelling: %v", err)
	}
	for k, v := range map[string]string{"algorithm": "random", "client-conn-throttle": "21", "nodebalancer": "Edge_1", "check-interval": "1", "nodebalancer-type": "huge"} {
		lb.Annotations = map[string]string{"linode.kevaso.com/" + k: v}
		var ae *kplugin.AnnotationError
		if _, err := p.EnsureLoadBalancer(ctx, lb); !errors.As(err, &ae) {
			t.Errorf("%s: %s taken: %v", k, v, err)
		}
	}
	lb.Annotations = map[string]string{"linode.kevaso.com/check-interval": "5", "linode.kevaso.com/check-timeout": "5"}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "below") {
		t.Fatalf("a timeout as long as the interval: %v", err)
	}
	if len(f.nbs) != 0 {
		t.Fatalf("made on a refused service: %v", f.nbs)
	}
	// Installed as linode-work: its domain; other domains aren't its business.
	lb.Annotations = map[string]string{
		"linode-work.kevaso.com/nodebalancer-type":    "premium",
		"linode-work.kevaso.com/client-conn-throttle": "7",
		"linode-work.kevaso.com/algorithm":            "leastconn",
		"linode-work.kevaso.com/check-attempts":       "4",
		"example.com/other":                           "ignored",
	}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil {
		t.Fatal(err)
	}
	for id, nb := range f.nbs {
		if nb["type"] != "premium" || fmt.Sprint(nb["client_conn_throttle"]) != "7" {
			t.Errorf("NodeBalancer %v", nb)
		}
		if c := f.configs[id][0]; c["algorithm"] != "leastconn" || fmt.Sprint(c["check_attempts"]) != "4" || c["check"] != "connection" {
			t.Errorf("config %v", c)
		}
	}
	// The throttle changes in place; the type is fixed.
	lb.Annotations["linode-work.kevaso.com/client-conn-throttle"] = "0"
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil {
		t.Fatal(err)
	}
	for _, nb := range f.nbs {
		if fmt.Sprint(nb["client_conn_throttle"]) != "0" {
			t.Errorf("throttle not updated: %v", nb)
		}
	}
	lb.Annotations["linode-work.kevaso.com/nodebalancer-type"] = "common"
	if _, err := p.EnsureLoadBalancer(ctx, lb); !errors.Is(err, kplugin.ErrConflict) {
		t.Fatalf("a type change: %v", err)
	}
}

// Before a silo is ready the service has no backend: its NodeBalancer is made in the provider's
// only region (its address known early), or waits when there are several; a shared one keeps its
// region (a service elsewhere is refused).
func TestNodeBalancerRegion(t *testing.T) {
	f, p := newFakeAPI(t)
	lb := service("uid-1/web", "shop", "web", nil, 443)
	lb.Backends, lb.Hosts = nil, []kplugin.HostInfo{torA, ord}
	if _, err := p.EnsureLoadBalancer(ctx, lb); !errors.Is(err, kplugin.ErrInProgress) || len(f.nbs) != 0 {
		t.Fatalf("no backend, two regions: %v", err)
	}
	lb.Hosts = []kplugin.HostInfo{torA, torB}
	st, err := p.EnsureLoadBalancer(ctx, lb)
	if err != nil || st.Address == "" {
		t.Fatalf("no backend, one region: %+v %v", st, err)
	}
	if got := configsOf(f, NodeBalancerLabel(lb.ID)); fmt.Sprint(got) != "[443→]" {
		t.Fatalf("no backend yet: %v", got)
	}
	lb.Backends = []kplugin.HostInfo{torA, ord}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "one region") {
		t.Fatalf("backends in two regions: %v", err)
	}
	edge := map[string]string{"linode.kevaso.com/nodebalancer": "edge"}
	if _, err := p.EnsureLoadBalancer(ctx, service("uid-2/a", "shop", "a", edge, 80)); err != nil {
		t.Fatal(err)
	}
	far := service("uid-3/b", "shop", "b", edge, 81)
	far.Backends = []kplugin.HostInfo{ord}
	if _, err := p.EnsureLoadBalancer(ctx, far); !errors.Is(err, kplugin.ErrConflict) || !strings.Contains(err.Error(), "ca-central") {
		t.Fatalf("a service in another region on edge: %v", err)
	}
}

// A Provider IP following its silo: reserved, shared with the region's Linodes, assigned to the
// first holder's Linode; the silo moves (the confirmed move core makes) and the new holder's
// Linode can announce it at once, since it is shared there already; a host in another region is
// refused (out of reach).
func TestProviderIPFollowsTheSilo(t *testing.T) {
	f, p := newFakeAPI(t)
	f.addLinode(11, "ca-central", "172.105.1.1")
	f.addLinode(12, "ca-central", "172.105.2.2")
	f.addLinode(21, "us-ord", "172.232.1.1")
	hosts := []kplugin.HostInfo{torA, torB, ord}
	st, err := p.ReserveAddress(ctx, kplugin.AddressReservation{Class: ClassSharedIP, ID: "ip-uid", Namespace: "payments", Name: "api-edge", Region: "ca-central"})
	if err != nil {
		t.Fatal(err)
	}
	lb := kplugin.LoadBalancer{Class: ClassSharedIP, ID: "ip-uid", Namespace: "payments", Name: "api-edge", Address: st.Address, Backends: []kplugin.HostInfo{torA}, Hosts: hosts}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil {
		t.Fatal(err)
	}
	if f.ips[st.Address]["linode_id"] != 11 || fmt.Sprint(f.shared[12]) != "["+st.Address+"]" || len(f.shared[21]) != 0 {
		t.Fatalf("first holder on tor-a: owner %v, shared %v", f.ips[st.Address]["linode_id"], f.shared)
	}
	shares := count(f.calls, "POST /networking/ips/share")
	lb.Backends = []kplugin.HostInfo{torB}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err != nil || count(f.calls, "POST /networking/ips/share") != shares {
		t.Fatalf("moved to tor-b: %v (shares %d → %d)", err, shares, count(f.calls, "POST /networking/ips/share"))
	}
	lb.Backends = []kplugin.HostInfo{ord}
	if _, err := p.EnsureLoadBalancer(ctx, lb); err == nil || !strings.Contains(err.Error(), "region only") {
		t.Fatalf("a move out of the region: %v", err)
	}
	if err := p.ReleaseAddress(ctx, kplugin.AddressRef{Class: ClassSharedIP, ID: "ip-uid", Address: st.Address, Handle: st.Handle}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.ips[st.Address]; ok || len(f.shared[12]) != 0 {
		t.Fatalf("after release: %v %v", f.ips[st.Address], f.shared)
	}
}
