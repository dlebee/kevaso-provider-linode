package linode

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// OwnerPrefix names the TXT record next to each record set saying who owns it (like external-dns'
// registry): _kevaso-owner.NAME holds the owner. A record set without it, or with another owner,
// is never changed.
const OwnerPrefix = "_kevaso-owner"

// zone finds the Linode DNS zone holding name (the longest one that's a suffix) and name relative
// to it ("" at the apex).
func (p *Provider) zone(ctx context.Context, name string) (*linodego.Domain, string, error) {
	ds, err := p.API.ListDomains(ctx, nil)
	if err != nil {
		return nil, "", apiErr("list DNS zones", err)
	}
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	var best *linodego.Domain
	for i := range ds {
		d := strings.ToLower(ds[i].Domain)
		if (name == d || strings.HasSuffix(name, "."+d)) && (best == nil || len(d) > len(best.Domain)) {
			best = &ds[i]
		}
	}
	if best == nil {
		return nil, "", fmt.Errorf("no Linode DNS zone holds %s (add the zone in Cloud Manager, Domains)", name)
	}
	rel := strings.TrimSuffix(strings.TrimSuffix(name, strings.ToLower(best.Domain)), ".")
	return best, rel, nil
}

func ownerName(rel string) string {
	if rel == "" {
		return OwnerPrefix
	}
	return OwnerPrefix + "." + rel
}

func unquote(s string) string { return strings.Trim(s, `"`) }

// recordSet is what Linode has at one name.
type recordSet struct {
	owner  *linodego.DomainRecord
	byType map[string][]linodego.DomainRecord
}

func (p *Provider) records(ctx context.Context, d *linodego.Domain, rel string) (recordSet, error) {
	rs, err := p.API.ListDomainRecords(ctx, d.ID, nil)
	if err != nil {
		return recordSet{}, apiErr("list records of "+d.Domain, err)
	}
	out := recordSet{byType: map[string][]linodego.DomainRecord{}}
	on := ownerName(rel)
	for i := range rs {
		r := rs[i]
		n := strings.ToLower(r.Name)
		switch {
		case n == on && r.Type == linodego.RecordTypeTXT:
			out.owner = &rs[i]
		case n == rel:
			out.byType[string(r.Type)] = append(out.byType[string(r.Type)], r)
		}
	}
	return out, nil
}

func (rs recordSet) ownedBy(owner string) (bool, string) {
	if rs.owner == nil {
		return false, ""
	}
	o := unquote(rs.owner.Target)
	return o == owner, o
}

// EnsureRecord implements kplugin.DNS: name/type gets exactly r.Values.
func (p *Provider) EnsureRecord(ctx context.Context, r kplugin.Record) error {
	if r.Provider != DNSName && r.Provider != "" {
		return fmt.Errorf("DNS provider %s: %w", r.Provider, kplugin.ErrUnsupported)
	}
	if r.Owner == "" {
		return fmt.Errorf("record %s: no owner", r.Name)
	}
	d, rel, err := p.zone(ctx, r.Name)
	if err != nil {
		return err
	}
	rs, err := p.records(ctx, d, rel)
	if err != nil {
		return err
	}
	mine, who := rs.ownedBy(r.Owner)
	switch {
	case rs.owner != nil && !mine:
		return fmt.Errorf("%s %s belongs to %q: %w", r.Name, r.Type, who, kplugin.ErrConflict)
	case rs.owner == nil && len(rs.byType[r.Type]) > 0:
		return fmt.Errorf("%s already has %s records not made by Kevaso: %w", r.Name, r.Type, kplugin.ErrConflict)
	case rs.owner == nil:
		if _, err := p.API.CreateDomainRecord(ctx, d.ID, linodego.DomainRecordCreateOptions{Type: linodego.RecordTypeTXT,
			Name: ownerName(rel), Target: r.Owner, TTLSec: 300}); err != nil {
			return apiErr("owner record for "+r.Name, err)
		}
	}
	want := map[string]bool{}
	for _, v := range r.Values {
		want[v] = true
	}
	have := map[string]bool{}
	for _, x := range rs.byType[r.Type] {
		v := x.Target
		if r.Type == "TXT" {
			v = unquote(v)
		}
		if !want[v] || have[v] {
			if err := p.API.DeleteDomainRecord(ctx, d.ID, x.ID); err != nil && !isNotFound(err) {
				return apiErr("delete "+r.Name+" "+r.Type, err)
			}
			continue
		}
		have[v] = true
		if r.TTL > 0 && x.TTLSec != r.TTL {
			ttl := r.TTL
			if _, err := p.API.UpdateDomainRecord(ctx, d.ID, x.ID, linodego.DomainRecordUpdateOptions{TTLSec: ttl}); err != nil {
				return apiErr("update "+r.Name+" "+r.Type, err)
			}
		}
	}
	vals := make([]string, 0, len(want))
	for v := range want {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	for _, v := range vals {
		if have[v] {
			continue
		}
		if _, err := p.API.CreateDomainRecord(ctx, d.ID, linodego.DomainRecordCreateOptions{Type: linodego.DomainRecordType(r.Type),
			Name: rel, Target: v, TTLSec: r.TTL}); err != nil {
			return apiErr("create "+r.Name+" "+r.Type, err)
		}
	}
	return nil
}

// DeleteRecord implements kplugin.DNS: removes name/type, and the owner record once nothing of the
// owner's is left there.
func (p *Provider) DeleteRecord(ctx context.Context, r kplugin.Record) error {
	d, rel, err := p.zone(ctx, r.Name)
	if err != nil {
		return err
	}
	rs, err := p.records(ctx, d, rel)
	if err != nil {
		return err
	}
	mine, who := rs.ownedBy(r.Owner)
	if rs.owner == nil {
		return kplugin.ErrNotFound
	}
	if !mine {
		return fmt.Errorf("%s belongs to %q: %w", r.Name, who, kplugin.ErrConflict)
	}
	gone := len(rs.byType[r.Type]) == 0
	for _, x := range rs.byType[r.Type] {
		if err := p.API.DeleteDomainRecord(ctx, d.ID, x.ID); err != nil && !isNotFound(err) {
			return apiErr("delete "+r.Name+" "+r.Type, err)
		}
	}
	left := false
	for typ, xs := range rs.byType {
		if typ != r.Type && (typ == "A" || typ == "AAAA" || typ == "TXT") && len(xs) > 0 {
			left = true
		}
	}
	if !left {
		if err := p.API.DeleteDomainRecord(ctx, d.ID, rs.owner.ID); err != nil && !isNotFound(err) {
			return apiErr("delete owner record of "+r.Name, err)
		}
	}
	if gone {
		return kplugin.ErrNotFound
	}
	return nil
}
