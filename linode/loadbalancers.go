package linode

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// NodeBalancerLabel is the NodeBalancer's label for a silo: found again by it (labels are unique in
// an account; 32 characters at most).
func NodeBalancerLabel(id string) string {
	l := "kvo-" + strings.ReplaceAll(id, "-", "")
	if len(l) > 32 {
		l = l[:32]
	}
	return l
}

// EnsureLoadBalancer implements kplugin.LoadBalancers.
func (p *Provider) EnsureLoadBalancer(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	switch lb.Class {
	case ClassNodeBalancer:
		return p.ensureNodeBalancer(ctx, lb)
	case ClassSharedIP:
		return p.ensureSharedIP(ctx, lb)
	}
	return kplugin.LoadBalancerStatus{}, fmt.Errorf("load-balancer class %s: %w", lb.Class, kplugin.ErrUnsupported)
}

// DeleteLoadBalancer implements kplugin.LoadBalancers.
func (p *Provider) DeleteLoadBalancer(ctx context.Context, ref kplugin.LoadBalancerRef) error {
	switch ref.Class {
	case ClassNodeBalancer:
		nb, err := p.findNodeBalancer(ctx, p.label(NodeBalancerLabel(ref.ID)))
		if err != nil {
			return err
		}
		if nb == nil {
			return kplugin.ErrNotFound
		}
		return apiErr("delete NodeBalancer "+*nb.Label, p.API.DeleteNodeBalancer(ctx, nb.ID))
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

func (p *Provider) ensureNodeBalancer(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	if len(lb.Backends) == 0 {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("NodeBalancer for %s/%s: no backend host", lb.Namespace, lb.Name)
	}
	region := lb.Backends[0].Region
	var nodes []linodego.NodeBalancerNodeCreateOptions
	for _, b := range lb.Backends {
		if b.Region != region {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("NodeBalancer backends span %s and %s: a NodeBalancer reaches one region", region, b.Region)
		}
		if b.PrivateAddress == "" {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("host %s (Linode %s) has no private IPv4: a NodeBalancer reaches hosts over the private network (add one in Cloud Manager, then reboot the Linode)", b.Name, b.ID)
		}
		nodes = append(nodes, linodego.NodeBalancerNodeCreateOptions{Label: b.Name, Weight: 100, Mode: linodego.ModeAccept})
	}
	for _, port := range lb.Ports {
		if !strings.EqualFold(port.Protocol, "TCP") && port.Protocol != "" {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("port %s: %s NodeBalancers forward TCP here (UDP: use %s)", port.Name, port.Protocol, ClassSharedIP)
		}
		if port.NodePort == 0 {
			return kplugin.LoadBalancerStatus{}, fmt.Errorf("port %s has no node port yet", port.Name)
		}
	}
	label := p.label(NodeBalancerLabel(lb.ID))
	nb, err := p.findNodeBalancer(ctx, label)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	if nb == nil {
		nb, err = p.API.CreateNodeBalancer(ctx, linodego.NodeBalancerCreateOptions{Label: &label, Region: region,
			Tags: append(append([]string(nil), p.Tags...), "kevaso-silo:"+lb.Namespace+"/"+lb.Name)})
		if err != nil {
			return kplugin.LoadBalancerStatus{}, apiErr("create NodeBalancer "+label, err)
		}
	}
	if nb.Region != region {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("NodeBalancer %s is in %s; the silo's host is in %s", label, nb.Region, region)
	}
	cfgs, err := p.API.ListNodeBalancerConfigs(ctx, nb.ID, nil)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, apiErr("NodeBalancer configs", err)
	}
	byPort := map[int]linodego.NodeBalancerConfig{}
	for _, c := range cfgs {
		byPort[c.Port] = c
	}
	want := map[int]bool{}
	for _, port := range lb.Ports {
		want[port.Port] = true
		ns := make([]linodego.NodeBalancerNodeCreateOptions, len(nodes))
		for i, n := range nodes {
			n.Address = lb.Backends[i].PrivateAddress + ":" + strconv.Itoa(port.NodePort)
			ns[i] = n
		}
		opts := linodego.NodeBalancerConfigRebuildOptions{Port: port.Port, Protocol: linodego.ProtocolTCP,
			Algorithm: linodego.AlgorithmRoundRobin, Check: linodego.CheckConnection, CheckInterval: 5, CheckTimeout: 3, CheckAttempts: 2}
		for _, n := range ns {
			opts.Nodes = append(opts.Nodes, linodego.NodeBalancerConfigRebuildNodeOptions{NodeBalancerNodeCreateOptions: n})
		}
		c, ok := byPort[port.Port]
		if !ok {
			created, err := p.API.CreateNodeBalancerConfig(ctx, nb.ID, linodego.NodeBalancerConfigCreateOptions{Port: port.Port,
				Protocol: linodego.ProtocolTCP, Algorithm: linodego.AlgorithmRoundRobin, Check: linodego.CheckConnection,
				CheckInterval: 5, CheckTimeout: 3, CheckAttempts: 2})
			if err != nil {
				return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("NodeBalancer %s port %d", label, port.Port), err)
			}
			c = *created
		}
		if _, err := p.API.RebuildNodeBalancerConfig(ctx, nb.ID, c.ID, opts); err != nil {
			return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("NodeBalancer %s port %d backends", label, port.Port), err)
		}
	}
	for port, c := range byPort {
		if !want[port] {
			if err := p.API.DeleteNodeBalancerConfig(ctx, nb.ID, c.ID); err != nil && !isNotFound(err) {
				return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("NodeBalancer %s port %d", label, port), err)
			}
		}
	}
	if nb.IPv4 == nil || *nb.IPv4 == "" {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("NodeBalancer %s has no IPv4 yet: %w", label, kplugin.ErrInProgress)
	}
	return kplugin.LoadBalancerStatus{Address: *nb.IPv4, SourceRanges: []string{NodeBalancerSources}}, nil
}

// ensureSharedIP shares lb.Address (an IPv4 of a Linode in the account, in the hosts' region) with
// every Linode host in that region, so whichever of them has the silo can announce it.
func (p *Provider) ensureSharedIP(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	addr, err := netip.ParseAddr(lb.Address)
	if err != nil || !addr.Is4() {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s needs loadBalancer.address: an IPv4 of a Linode in your account (extra IPv4s are asked for through Linode support)", ClassSharedIP)
	}
	ip, err := p.API.GetIPAddress(ctx, addr.String())
	if err != nil {
		return kplugin.LoadBalancerStatus{}, apiErr("IP address "+addr.String()+" (is it in this account?)", err)
	}
	if len(lb.Backends) == 0 {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("shared IP %s: no backend host", addr)
	}
	if b := lb.Backends[0]; b.Region != ip.Region {
		return kplugin.LoadBalancerStatus{}, fmt.Errorf("%s is in %s; the silo's host %s is in %s: put the silo in %s (placement.hostSelector or region)", addr, ip.Region, b.Name, b.Region, ip.Region)
	}
	for _, h := range lb.Hosts {
		if h.Region != ip.Region {
			continue
		}
		id, err := linodeID(h)
		if err != nil {
			return kplugin.LoadBalancerStatus{}, err
		}
		if id == ip.LinodeID {
			continue // its own address
		}
		if err := p.setShared(ctx, id, func(cur []string) []string {
			for _, c := range cur {
				if c == addr.String() {
					return nil
				}
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

// unshare stops sharing addr with every Linode in its region.
func (p *Provider) unshare(ctx context.Context, address string) error {
	if address == "" {
		return kplugin.ErrNotFound
	}
	ip, err := p.API.GetIPAddress(ctx, address)
	if err != nil {
		return apiErr("IP address "+address, err)
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
			out := []string{}
			found := false
			for _, c := range cur {
				if c == address {
					found = true
					continue
				}
				out = append(out, c)
			}
			if !found {
				return nil
			}
			return out
		}); err != nil {
			return err
		}
	}
	return nil
}
