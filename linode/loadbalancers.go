package linode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// NodeBalancers (networking v2): one per LoadBalancer service by default, like Kubernetes on
// Linode. Services of one namespace naming the same nodebalancer annotation share one, each on its
// own ports. Core calls EnsureLoadBalancer once per service (lb.ID stable for the service); which
// NodeBalancer it is on, and which ports are its, are tags on the NodeBalancer:
//
//	kvo-svc-HASH         a service on it (HASH: of its ID)
//	kvo-port-PORT-HASH   that port is that service's
//
// so it is found again after a restart, a port is never taken from another service, and the
// NodeBalancer is created with its first service and deleted with its last.

func hashOf(s string, n int) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:n]
}

func svcTag(id string) string { return "kvo-svc-" + hashOf(id, 12) }
func portTag(port int, id string) string {
	return "kvo-port-" + strconv.Itoa(port) + "-" + hashOf(id, 12)
}

// portOwner reads a port tag: its port and service hash.
func portOwner(tag string) (int, string, bool) {
	rest, ok := strings.CutPrefix(tag, "kvo-port-")
	if !ok {
		return 0, "", false
	}
	p, h, ok := strings.Cut(rest, "-")
	n, err := strconv.Atoi(p)
	if !ok || err != nil {
		return 0, "", false
	}
	return n, h, true
}

// NodeBalancerLabel is the label of a service's own NodeBalancer (by the service's ID): found
// again by it (labels are unique in an account; 32 characters at most).
func NodeBalancerLabel(id string) string { return "kvo-" + hashOf(id, 24) }

// SharedNodeBalancerLabel is the label of the NodeBalancer named name in namespace ns.
func SharedNodeBalancerLabel(ns, name string) string {
	return "kvo-" + name + "-" + hashOf(ns+"/"+name, 6)
}

// nodeLabel is a NodeBalancer backend's label. Linode requires 3–32 characters (a real API 400:
// [nodes[0].label] Length must be 3-32 characters). A host name can be shorter (w1) or longer.
// A short name is prefixed; a long one keeps a hash of the whole name so two don't share a label.
func nodeLabel(name string) string {
	const minL, maxL = 3, 32
	if len(name) >= minL && len(name) <= maxL {
		return name
	}
	if len(name) < minL {
		l := "kvo-" + name
		if len(l) < minL {
			return "kvo-node"
		}
		return l
	}
	suf := hashOf(name, 8)
	head := name
	if room := maxL - 1 - len(suf); len(head) > room {
		head = head[:room]
	}
	return strings.TrimRight(head, "-") + "-" + suf
}

// nbLock is the lock of one NodeBalancer (by label) around a read-change-write of its tags and
// configs: two services on a shared one must not lose each other's change.
func (p *Provider) nbLock(label string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nbLocks == nil {
		p.nbLocks = map[string]*sync.Mutex{}
	}
	if p.nbLocks[label] == nil {
		p.nbLocks[label] = &sync.Mutex{}
	}
	return p.nbLocks[label]
}

// EnsureLoadBalancer implements kplugin.LoadBalancers.
func (p *Provider) EnsureLoadBalancer(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	switch lb.Class {
	case ClassNodeBalancer:
		return p.ensureNodeBalancer(ctx, lb)
	case ClassSharedIP:
		return p.ensureSharedIP(ctx, lb)
	}
	return kplugin.LoadBalancerStatus{}, fmt.Errorf("load-balancer class %s: %w", lb.Class, kplugin.ErrUnsupported)
}

// DeleteLoadBalancer implements kplugin.LoadBalancers: a service leaves its NodeBalancer (deleted
// if it was the last), or a Provider IP is no longer shared.
func (p *Provider) DeleteLoadBalancer(ctx context.Context, ref kplugin.LoadBalancerRef) error {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	switch ref.Class {
	case ClassNodeBalancer:
		left, err := p.leaveNodeBalancers(ctx, ref.ID, "")
		if err != nil {
			return err
		}
		if left == 0 {
			return kplugin.ErrNotFound
		}
		return nil
	case ClassSharedIP:
		return p.unshare(ctx, ref.Address)
	}
	return fmt.Errorf("load-balancer class %s: %w", ref.Class, kplugin.ErrUnsupported)
}

func (p *Provider) findNodeBalancer(ctx context.Context, label string) (*linodego.NodeBalancer, error) {
	nbs, err := p.API.ListNodeBalancers(ctx, filter(map[string]any{"label": label}))
	if err != nil {
		return nil, apiErr("list NodeBalancers", err)
	}
	for i := range nbs {
		if nbs[i].Label != nil && *nbs[i].Label == label {
			return &nbs[i], nil
		}
	}
	return nil, nil
}

// knobs are a service's NodeBalancer settings, from its annotations.
type knobs struct {
	label    string // which NodeBalancer
	shared   string // its name, when shared
	typ      linodego.NodeBalancerPlanType
	throttle *int
	algo     linodego.ConfigAlgorithm
	proxy    linodego.ConfigProxyProtocol
	check    linodego.ConfigCheck
	interval int
	timeout  int
	attempts int
}

func (p *Provider) knobsOf(lb kplugin.LoadBalancer) (knobs, error) {
	s, err := settings(kplugin.OnService, lb.Annotations)
	if err != nil {
		return knobs{}, err
	}
	k := knobs{label: p.label(NodeBalancerLabel(lb.ID)), algo: linodego.AlgorithmRoundRobin, proxy: linodego.ProxyProtocolNone,
		check: linodego.CheckConnection, interval: 5, timeout: 3, attempts: 2}
	if name := s[KeyNodeBalancer]; name != "" {
		k.shared, k.label = name, p.label(SharedNodeBalancerLabel(lb.Namespace, name))
	}
	if t := s[KeyNodeBalancerType]; t != "" {
		k.typ = linodego.NodeBalancerPlanType(t)
	}
	num := func(key string, into *int) {
		if v, ok := s[key]; ok {
			*into, _ = strconv.Atoi(v) // validated
		}
	}
	if _, ok := s[KeyClientConnThrottle]; ok {
		k.throttle = new(int)
		num(KeyClientConnThrottle, k.throttle)
	}
	if v := s[KeyAlgorithm]; v != "" {
		k.algo = linodego.ConfigAlgorithm(v)
	}
	if v := s[KeyProxyProtocol]; v != "" {
		k.proxy = linodego.ConfigProxyProtocol(v)
	}
	if v := s[KeyCheck]; v != "" {
		k.check = linodego.ConfigCheck(v)
	}
	num(KeyCheckInterval, &k.interval)
	num(KeyCheckTimeout, &k.timeout)
	num(KeyCheckAttempts, &k.attempts)
	if k.timeout >= k.interval {
		return knobs{}, fmt.Errorf("%s (%ds) must be below %s (%ds)", KeyCheckTimeout, k.timeout, KeyCheckInterval, k.interval)
	}
	return k, nil
}

// regionOf is where the service's NodeBalancer goes: its backends' region; with none yet, the
// existing NodeBalancer's, or the provider's hosts' if they are all in one.
func regionOf(lb kplugin.LoadBalancer, nb *linodego.NodeBalancer) (string, error) {
	var region string
	backends := slices.Clone(lb.Backends)
	for _, port := range lb.Ports {
		backends = append(backends, port.Backends...)
	}
	for _, b := range backends {
		if region != "" && b.Region != region {
			return "", fmt.Errorf("service %s/%s: backends in %s and %s: a NodeBalancer reaches one region", lb.Namespace, lb.Name, region, b.Region)
		}
		region = b.Region
		if b.PrivateAddress == "" {
			return "", fmt.Errorf("host %s (Linode %s) has no private IPv4: a NodeBalancer reaches hosts over the private network (add one in Cloud Manager, then reboot the Linode)", b.Name, b.ID)
		}
	}
	if region != "" {
		return region, nil
	}
	if nb != nil {
		return nb.Region, nil
	}
	regions := map[string]bool{}
	for _, h := range lb.Hosts {
		regions[h.Region] = true
	}
	if len(regions) == 1 {
		for r := range regions {
			return r, nil
		}
	}
	return "", fmt.Errorf("service %s/%s: no ready silo yet to put its NodeBalancer in the silo's region: %w", lb.Namespace, lb.Name, kplugin.ErrInProgress)
}

// ensureNodeBalancer puts the service's ports on its NodeBalancer (its own, or the shared one its
// annotation names), each port to its backends' node port over the region's private network.
func (p *Provider) ensureNodeBalancer(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	if lb.ID == "" {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("NodeBalancer for %s/%s: no ID", lb.Namespace, lb.Name)
	}
	k, err := p.knobsOf(lb)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	seen := map[int]bool{}
	for _, port := range lb.Ports {
		if !strings.EqualFold(port.Protocol, "TCP") && port.Protocol != "" {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("port %s of %s/%s: %s: NodeBalancers forward TCP here (UDP: a Provider IP, hostPort: { ip })", port.Name, lb.Namespace, lb.Name, port.Protocol)
		}
		if port.NodePort == 0 {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("port %s has no node port yet: %w", port.Name, kplugin.ErrInProgress)
		}
		if seen[port.Port] {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("port %d twice in %s/%s", port.Port, lb.Namespace, lb.Name)
		}
		seen[port.Port] = true
		if _, err := settings(kplugin.OnService, port.Annotations); err != nil {
			return kplugin.LoadBalancerStatus{}, err
		}
	}
	st, err := p.ensureOn(ctx, lb, k)
	if err != nil {
		return st, err
	}
	// Moved (its nodebalancer annotation changed): off the others.
	if _, err := p.leaveNodeBalancers(ctx, lb.ID, k.label); err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	return st, nil
}

func (p *Provider) ensureOn(ctx context.Context, lb kplugin.LoadBalancer, k knobs) (kplugin.LoadBalancerStatus, error) {
	l := p.nbLock(k.label)
	l.Lock()
	defer l.Unlock()
	nb, err := p.findNodeBalancer(ctx, k.label)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	region, err := regionOf(lb, nb)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	me := hashOf(lb.ID, 12)
	want := []string{svcTag(lb.ID)}
	for _, port := range lb.Ports {
		want = append(want, portTag(port.Port, lb.ID))
	}
	what := "NodeBalancer " + k.label
	if k.shared != "" {
		what = "NodeBalancer " + k.shared + " (" + k.label + ")"
	}
	if nb == nil {
		opts := linodego.NodeBalancerCreateOptions{Label: &k.label, Region: region, ClientConnThrottle: k.throttle, Type: k.typ,
			Tags: append(slices.Clone(p.Tags), want...)}
		if nb, err = p.API.CreateNodeBalancer(ctx, opts); err != nil {
			return kplugin.LoadBalancerStatus{}, apiErr("create "+what, err)
		}
	} else {
		if nb.Region != region {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s is in %s; %s/%s's silos are in %s: %w", what, nb.Region, lb.Namespace, lb.Name, region, kplugin.ErrConflict)
		}
		if k.typ != "" && nb.Type != "" && nb.Type != k.typ {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s is %s, not %s: the type is fixed when it is made (remove every service on it to make it again): %w", what, nb.Type, k.typ, kplugin.ErrConflict)
		}
		for _, t := range nb.Tags {
			if port, h, ok := portOwner(t); ok && h != me && hasPort(lb, port) {
				return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s: port %d is another service's: one service per port on a shared NodeBalancer: %w", what, port, kplugin.ErrConflict)
			}
		}
		next := slices.Clone(p.Tags)
		for _, t := range nb.Tags {
			if _, h, ok := portOwner(t); (ok && h == me) || t == svcTag(lb.ID) || slices.Contains(next, t) {
				continue // this service's: rewritten below
			}
			next = append(next, t)
		}
		next = append(next, want...)
		upd := linodego.NodeBalancerUpdateOptions{}
		if !sameSet(next, nb.Tags) {
			upd.Tags = &next
		}
		if k.throttle != nil && *k.throttle != nb.ClientConnThrottle {
			upd.ClientConnThrottle = k.throttle
		}
		if upd.Tags != nil || upd.ClientConnThrottle != nil {
			if nb, err = p.API.UpdateNodeBalancer(ctx, nb.ID, upd); err != nil {
				return kplugin.LoadBalancerStatus{}, apiErr("update "+what, err)
			}
		}
	}
	// The configs: this service's ports rebuilt, its old ones and ports nobody owns removed.
	owned := map[int]bool{}
	for _, t := range nb.Tags {
		if port, _, ok := portOwner(t); ok {
			owned[port] = true
		}
	}
	cfgs, err := p.API.ListNodeBalancerConfigs(ctx, nb.ID, nil)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, apiErr(what+" configs", err)
	}
	byPort := map[int]linodego.NodeBalancerConfig{}
	for _, c := range cfgs {
		byPort[c.Port] = c
	}
	for _, port := range lb.Ports {
		backends := port.Backends
		if len(backends) == 0 {
			backends = lb.Backends
		}
		opts := linodego.NodeBalancerConfigRebuildOptions{Port: port.Port, Protocol: linodego.ProtocolTCP, ProxyProtocol: k.proxy,
			Algorithm: k.algo, Check: k.check, CheckInterval: k.interval, CheckTimeout: k.timeout, CheckAttempts: k.attempts,
			Nodes: []linodego.NodeBalancerConfigRebuildNodeOptions{}}
		for _, b := range backends {
			opts.Nodes = append(opts.Nodes, linodego.NodeBalancerConfigRebuildNodeOptions{NodeBalancerNodeCreateOptions: linodego.NodeBalancerNodeCreateOptions{
				Label: nodeLabel(b.Name), Weight: 100, Mode: linodego.ModeAccept, Address: b.PrivateAddress + ":" + strconv.Itoa(port.NodePort)}})
		}
		c, ok := byPort[port.Port]
		if !ok {
			created, err := p.API.CreateNodeBalancerConfig(ctx, nb.ID, linodego.NodeBalancerConfigCreateOptions{Port: port.Port,
				Protocol: linodego.ProtocolTCP, ProxyProtocol: k.proxy, Algorithm: k.algo, Check: k.check,
				CheckInterval: k.interval, CheckTimeout: k.timeout, CheckAttempts: k.attempts})
			if err != nil {
				return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("%s port %d", what, port.Port), err)
			}
			c = *created
		}
		if _, err := p.API.RebuildNodeBalancerConfig(ctx, nb.ID, c.ID, opts); err != nil {
			return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("%s port %d backends", what, port.Port), err)
		}
	}
	for port, c := range byPort {
		if !owned[port] {
			if err := p.API.DeleteNodeBalancerConfig(ctx, nb.ID, c.ID); err != nil && !isNotFound(err) {
				return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("%s port %d", what, port), err)
			}
		}
	}
	if nb.IPv4 == nil || *nb.IPv4 == "" {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s has no IPv4 yet: %w", what, kplugin.ErrInProgress)
	}
	st := kplugin.LoadBalancerStatus{Address: *nb.IPv4, SourceRanges: []string{NodeBalancerSources}}
	if nb.IPv6 != nil {
		st.AddressIPv6 = *nb.IPv6 // a NodeBalancer answers on IPv6 too, to backends on private IPv4 (E053)
	}
	return st, nil
}

func hasPort(lb kplugin.LoadBalancer, port int) bool {
	return slices.ContainsFunc(lb.Ports, func(p kplugin.Port) bool { return p.Port == port })
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	sort.Strings(a)
	sort.Strings(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}

// leaveNodeBalancers takes the service (by ID) off every NodeBalancer but keep: its ports and its
// tags go, and a NodeBalancer with no service left is deleted. It says how many it left.
func (p *Provider) leaveNodeBalancers(ctx context.Context, id, keep string) (int, error) {
	nbs, err := p.API.ListNodeBalancers(ctx, nil)
	if err != nil {
		return 0, apiErr("list NodeBalancers", err)
	}
	tag, me := svcTag(id), hashOf(id, 12)
	left := 0
	for _, nb := range nbs {
		if nb.Label == nil || *nb.Label == keep || !slices.Contains(nb.Tags, tag) {
			continue
		}
		if err := p.leave(ctx, *nb.Label, tag, me); err != nil {
			return left, err
		}
		left++
	}
	return left, nil
}

func (p *Provider) leave(ctx context.Context, label, tag, me string) error {
	l := p.nbLock(label)
	l.Lock()
	defer l.Unlock()
	nb, err := p.findNodeBalancer(ctx, label)
	if err != nil || nb == nil {
		return err
	}
	var next []string
	mine := map[int]bool{}
	others := false
	for _, t := range nb.Tags {
		if port, h, ok := portOwner(t); ok && h == me {
			mine[port] = true
			continue
		}
		if t == tag {
			continue
		}
		if strings.HasPrefix(t, "kvo-svc-") {
			others = true
		}
		next = append(next, t)
	}
	if !others {
		return apiErr("delete NodeBalancer "+label, p.API.DeleteNodeBalancer(ctx, nb.ID))
	}
	cfgs, err := p.API.ListNodeBalancerConfigs(ctx, nb.ID, nil)
	if err != nil {
		return apiErr("NodeBalancer "+label+" configs", err)
	}
	for _, c := range cfgs {
		if mine[c.Port] {
			if err := p.API.DeleteNodeBalancerConfig(ctx, nb.ID, c.ID); err != nil && !isNotFound(err) {
				return apiErr(fmt.Sprintf("NodeBalancer %s port %d", label, c.Port), err)
			}
		}
	}
	if _, err := p.API.UpdateNodeBalancer(ctx, nb.ID, linodego.NodeBalancerUpdateOptions{Tags: &next}); err != nil {
		return apiErr("update NodeBalancer "+label, err)
	}
	return nil
}

// ensureSharedIP shares lb.Address (a Provider IP: reserved in the hosts' region) with every
// Linode host in that region, so whichever of them has the silo can announce it. A reserved
// address no Linode has yet is assigned to the silo's host first.
func (p *Provider) ensureSharedIP(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	addr, err := netip.ParseAddr(lb.Address)
	if err != nil || !addr.Is4() {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s needs its IP's address (an IP of type Provider, reserved in a region)", ClassSharedIP)
	}
	ip, err := p.API.GetIPAddress(ctx, addr.String())
	if err != nil {
		return kplugin.LoadBalancerStatus{}, apiErr("IP address "+addr.String()+" (is it in this account?)", err)
	}
	if ok, err := p.kevasos(ctx, ip); err != nil {
		return kplugin.LoadBalancerStatus{}, err
	} else if !ok {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("IP %s isn't reserved: not sharing it (only addresses Kevaso reserved are shared)", addr)
	}
	if len(lb.Backends) == 0 {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("shared IP %s: no backend host", addr)
	}
	if b := lb.Backends[0]; b.Region != ip.Region {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s is in %s; the silo's host %s is in %s: a Provider IP reaches its region only", addr, ip.Region, b.Name, b.Region)
	}
	owner, err := p.assignIfUnassigned(ctx, ip, lb.Backends[0])
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	for _, h := range lb.Hosts {
		if h.Region != ip.Region {
			continue
		}
		id, err := linodeID(h)
		if err != nil {
			return kplugin.LoadBalancerStatus{}, err
		}
		if id == owner {
			continue // its own address
		}
		if err := p.setShared(ctx, id, func(cur []string) []string {
			if slices.Contains(cur, addr.String()) {
				return nil
			}
			return append(cur, addr.String())
		}); err != nil {
			return kplugin.LoadBalancerStatus{}, err
		}
	}
	return kplugin.LoadBalancerStatus{Address: addr.String()}, nil
}

// setShared changes the list of IPs shared with a Linode (the API replaces the whole list).
// change returns nil for "no change".
func (p *Provider) setShared(ctx context.Context, linode int, change func([]string) []string) error {
	l := p.sharingLock(linode)
	l.Lock()
	defer l.Unlock()
	ips, err := p.API.GetInstanceIPAddresses(ctx, linode)
	if err != nil {
		return apiErr(fmt.Sprintf("Linode %d's IPs", linode), err)
	}
	var cur []string
	if ips.IPv4 != nil {
		for _, s := range ips.IPv4.Shared {
			cur = append(cur, s.Address)
		}
	}
	sort.Strings(cur)
	next := change(append([]string(nil), cur...))
	if next == nil {
		return nil
	}
	if err := p.API.ShareIPAddresses(ctx, linodego.IPAddressesShareOptions{LinodeID: linode, IPs: next}); err != nil {
		return apiErr(fmt.Sprintf("share IPs with Linode %d", linode), err)
	}
	return nil
}

// unshare stops sharing addr with every Linode in its region, if Kevaso reserved it; any other
// address is left as it is.
func (p *Provider) unshare(ctx context.Context, address string) error {
	if address == "" {
		return kplugin.ErrNotFound
	}
	ip, err := p.API.GetIPAddress(ctx, address)
	if err != nil {
		return apiErr("IP address "+address, err)
	}
	if ok, err := p.kevasos(ctx, ip); err != nil {
		return err
	} else if !ok {
		return nil // not Kevaso's to unshare (K-044): letting it go leaves its sharing as it is
	}
	is, err := p.API.ListInstances(ctx, filter(map[string]any{"region": ip.Region}))
	if err != nil {
		return apiErr("list Linodes", err)
	}
	for _, in := range is {
		if in.ID == ip.LinodeID {
			continue
		}
		if err := p.setShared(ctx, in.ID, func(cur []string) []string {
			if !slices.Contains(cur, address) {
				return nil
			}
			return slices.DeleteFunc(cur, func(c string) bool { return c == address })
		}); err != nil {
			return err
		}
	}
	return nil
}
