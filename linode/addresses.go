package linode

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// Addresses held apart from a load balancer, for core's IP objects (design §7.2c-1).
//
//   - linode-shared-ip reserves: a Linode reserved IPv4 in the IP's region (POST
//     /networking/reserved/ips), which outlives every Linode; or, with an address asked for, an IPv4
//     already in the account, adopted and never given up. Sharing it with the hosts is
//     EnsureLoadBalancer's (the silo's host announces it); unsharing, DeleteLoadBalancer's.
//     Releasing unshares it everywhere and, if it reserved it, deletes the reservation.
//   - linode-nodebalancer can't: its address is the NodeBalancer's. One NodeBalancer per IP (its
//     label carries the IP's UID), a config per port of every service on the IP (they share it by
//     default), so it and its address last as long as the IP, across every backend change and
//     from holder to holder; with nobody holding the IP it is parked (no configs: it keeps its
//     address and sends nowhere), and releasing the IP deletes it.

// Handles tell a reserved address from one that was already the account's.
const (
	handleReserved = "reserved:"
	handleAdopted  = "adopted:"
)

// ReserveAddress implements kplugin.Addresses.
func (p *Provider) ReserveAddress(ctx context.Context, r kplugin.AddressReservation) (kplugin.AddressStatus, error) {
	if r.Class != ClassSharedIP {
		return kplugin.AddressStatus{}, fmt.Errorf("class %s holds no address apart from its load balancer: %w", r.Class, kplugin.ErrUnsupported)
	}
	for k := range r.Annotations {
		if strings.HasPrefix(k, "linode.kevaso.com/") {
			return kplugin.AddressStatus{}, fmt.Errorf("unknown annotation %s: a shared IP reads none: %w", k, kplugin.ErrUnsupported)
		}
	}
	if r.Address != "" {
		addr, err := netip.ParseAddr(r.Address)
		if err != nil || !addr.Is4() {
			return kplugin.AddressStatus{}, fmt.Errorf("%s: %q isn't an IPv4", ClassSharedIP, r.Address)
		}
		ip, err := p.API.GetIPAddress(ctx, addr.String())
		if err != nil {
			return kplugin.AddressStatus{}, apiErr("IP address "+addr.String()+" (is it in this account?)", err)
		}
		if r.Region != "" && ip.Region != r.Region {
			return kplugin.AddressStatus{}, fmt.Errorf("%s is in %s, not %s", addr, ip.Region, r.Region)
		}
		return kplugin.AddressStatus{Address: addr.String(), Region: ip.Region, Handle: handleAdopted + addr.String()}, nil
	}
	if r.Region == "" {
		return kplugin.AddressStatus{}, fmt.Errorf("reserving a %s address needs a region", ClassSharedIP)
	}
	// A reserved IP carries no label to find it again by, so the same ID asking again gets what
	// this process reserved for it (core writes it to the IP's status at once; a crash between the
	// two leaves an unused reservation in the account: see the README).
	p.mu.Lock()
	defer p.mu.Unlock()
	if st, ok := p.reservedFor[r.ID]; ok {
		return st, nil
	}
	ip, err := p.API.ReserveIPAddress(ctx, linodego.ReserveIPOptions{Region: r.Region})
	if err != nil {
		return kplugin.AddressStatus{}, apiErr("reserve an IPv4 in "+r.Region+" (reserved IPs may need enabling on the account)", err)
	}
	st := kplugin.AddressStatus{Address: ip.Address, Region: ip.Region, Handle: handleReserved + ip.Address}
	if p.reservedFor == nil {
		p.reservedFor = map[string]kplugin.AddressStatus{}
	}
	p.reservedFor[r.ID] = st
	return st, nil
}

// ReleaseAddress implements kplugin.Addresses.
func (p *Provider) ReleaseAddress(ctx context.Context, ref kplugin.AddressRef) error {
	switch ref.Class {
	case ClassNodeBalancer:
		return p.DeleteLoadBalancer(ctx, kplugin.LoadBalancerRef{Class: ref.Class, ID: ref.ID})
	case ClassSharedIP:
	default:
		return fmt.Errorf("class %s: %w", ref.Class, kplugin.ErrUnsupported)
	}
	addr := ref.Address
	if addr == "" {
		_, addr, _ = strings.Cut(ref.Handle, ":")
	}
	if addr == "" {
		return kplugin.ErrNotFound
	}
	if err := p.unshare(ctx, addr); err != nil {
		if errors.Is(err, kplugin.ErrNotFound) {
			return kplugin.ErrNotFound
		}
		return err
	}
	if !strings.HasPrefix(ref.Handle, handleReserved) {
		return nil // already the account's: it stays there, no longer Kevaso's to share
	}
	if err := p.API.DeleteReservedIPAddress(ctx, addr); err != nil {
		if isNotFound(err) {
			return kplugin.ErrNotFound
		}
		return apiErr("release reserved IP "+addr, err)
	}
	p.mu.Lock()
	delete(p.reservedFor, ref.ID)
	p.mu.Unlock()
	return nil
}

// parkNodeBalancer keeps an IP's NodeBalancer, and so its address, with nothing behind it: no
// configs, so no port forwards anywhere while nobody holds the IP.
func (p *Provider) parkNodeBalancer(ctx context.Context, lb kplugin.LoadBalancer) (kplugin.LoadBalancerStatus, error) {
	label := p.label(NodeBalancerLabel(lb.ID))
	nb, err := p.findNodeBalancer(ctx, label)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, err
	}
	if nb == nil {
		return kplugin.LoadBalancerStatus{}, kplugin.ErrNotFound
	}
	cfgs, err := p.API.ListNodeBalancerConfigs(ctx, nb.ID, nil)
	if err != nil {
		return kplugin.LoadBalancerStatus{}, apiErr("NodeBalancer configs", err)
	}
	for _, c := range cfgs {
		if err := p.API.DeleteNodeBalancerConfig(ctx, nb.ID, c.ID); err != nil && !isNotFound(err) {
			return kplugin.LoadBalancerStatus{}, apiErr(fmt.Sprintf("park NodeBalancer %s port %d", label, c.Port), err)
		}
	}
	st := kplugin.LoadBalancerStatus{SourceRanges: []string{NodeBalancerSources}}
	if nb.IPv4 != nil {
		st.Address = *nb.IPv4
	}
	return st, nil
}

// assignIfUnassigned gives a reserved address nobody has yet to the first backend host, so it can
// be shared from there with the region's others (a shared IPv4 is one Linode's, shared with the
// rest).
func (p *Provider) assignIfUnassigned(ctx context.Context, ip *linodego.InstanceIP, backend kplugin.HostInfo) (int, error) {
	if ip.LinodeID != 0 {
		return ip.LinodeID, nil
	}
	id, err := linodeID(backend)
	if err != nil {
		return 0, err
	}
	if err := p.API.InstancesAssignIPs(ctx, linodego.LinodesAssignIPsOptions{Region: ip.Region,
		Assignments: []linodego.LinodeIPAssignment{{Address: ip.Address, LinodeID: id}}}); err != nil {
		return 0, apiErr(fmt.Sprintf("assign %s to Linode %d", ip.Address, id), err)
	}
	return id, nil
}
