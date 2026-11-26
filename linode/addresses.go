package linode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/provider-linode/dcid"
)

// Provider IPs (networking v2, IP type: Provider): a Linode reserved IPv4 in the IP's region (POST
// /networking/reserved/ips), which outlives every Linode. Sharing it with the region's Linodes is
// EnsureLoadBalancer's (class linode-shared-ip: the silo's host announces it over BGP);
// unsharing, DeleteLoadBalancer's. Releasing unshares it everywhere and deletes the reservation.
// An address Kevaso didn't reserve is never shared or unshared (K-044).

// handleReserved starts the handle of a reserved address.
const handleReserved = "reserved:"

// ReserveAddress implements kplugin.Addresses.
func (p *Provider) ReserveAddress(ctx context.Context, r kplugin.AddressReservation) (kplugin.AddressStatus, error) {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	if r.Class != ClassSharedIP {
		return kplugin.AddressStatus{}, fmt.Errorf("class %s holds no address apart from its load balancer: %w", r.Class, kplugin.ErrUnsupported)
	}
	if _, err := settings(kplugin.OnIP, r.Annotations); err != nil {
		return kplugin.AddressStatus{}, err
	}
	if r.Address != "" {
		return kplugin.AddressStatus{}, fmt.Errorf("a Provider IP is reserved by Linode: it can't be asked for an address (%s)", r.Address)
	}
	if r.Region == "" {
		return kplugin.AddressStatus{}, fmt.Errorf("reserving a Provider IP needs a region")
	}
	if _, ok := dcid.IDs[r.Region]; !ok {
		return kplugin.AddressStatus{}, fmt.Errorf("region %s: Linode has no BGP failover there, so a shared IP can't follow a silo (regions: %s)", r.Region, strings.Join(dcid.Regions(), ", "))
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
	ctx, cancel := p.bound(ctx)
	defer cancel()
	switch ref.Class {
	case ClassNodeBalancer:
		// A NodeBalancer's address is its own: letting it go is the service leaving it (what
		// core's conformance kit, from before networking v2, still asks this way).
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

// kevasos says whether Kevaso may share or unshare ip: a reserved address (one Kevaso reserved;
// Linode marks them, and lists them under /networking/reserved/ips) (K-044).
// A machine's own address, or anything else of the account's, is left alone.
func (p *Provider) kevasos(ctx context.Context, ip *linodego.InstanceIP) (bool, error) {
	if ip.Reserved {
		return true, nil
	}
	p.mu.Lock()
	for _, st := range p.reservedFor {
		if st.Address == ip.Address {
			p.mu.Unlock()
			return true, nil
		}
	}
	p.mu.Unlock()
	if _, err := p.API.GetReservedIPAddress(ctx, ip.Address); err == nil {
		return true, nil
	} else if !isNotFound(err) {
		return false, apiErr("reserved IP "+ip.Address, err)
	}
	return false, nil
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
		if overLimit(err) {
			// The message ends with the fix: the sentinel's text is trimmed off over gRPC.
			return 0, fmt.Errorf("%s (Linode %d) can't take %s, which it shares from: Linode allows one public IPv4 per Linode "+
				"until its support raises the limit. Ask Linode support to allow more IPv4 addresses per Linode on this account: %w",
				backend.Name, id, ip.Address, kplugin.ErrConflict)
		}
		return 0, apiErr(fmt.Sprintf("assign %s to Linode %d", ip.Address, id), err)
	}
	return id, nil
}

// overLimit says whether Linode refused an assignment for the per-Linode IPv4 limit ([400]
// Assignment would exceed maximum for Linode N).
func overLimit(err error) bool {
	var le *linodego.Error
	return errors.As(err, &le) && le.Code == http.StatusBadRequest && strings.Contains(le.Error(), "exceed maximum")
}
