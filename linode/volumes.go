package linode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// Linode volumes are 10 GiB at least.
const minVolumeGiB = 10

func gib(bytes int64) int {
	g := int((bytes + (1<<30 - 1)) >> 30)
	if g < minVolumeGiB {
		g = minVolumeGiB
	}
	return g
}

// A volume's identity in Linode (audit K-035): core's volume ID (the silo's UID and the volume's
// name) is hashed, and the hash goes into three places, so no two volumes can ever share one Linode
// volume, and nothing acts on a Linode volume it can't tie to the volume it was asked about:
//
//   - the label: core's human name, cut to fit, then -HASH[:8]: unique for every ID;
//   - a tag, kvo-vol-HASH[:24], set at creation: a volume found by its label is adopted only with it;
//   - the handle, LINODEID:HASH[:24]: attach, detach and delete check the volume still carries that
//     tag (a Linode volume made outside Kevaso, or for another volume, is never touched).
const (
	identityTagPrefix = "kvo-vol-"
	labelHash         = 8
	tagHash           = 24
)

// identity is the hash of core's volume ID.
func identity(id string) string {
	h := sha256.Sum256([]byte(id))
	return hex.EncodeToString(h[:])
}

func identityTag(hash string) string { return identityTagPrefix + hash[:tagHash] }

// volumeLabel is the Linode label of the volume core calls name (kvo-NS-SILO-VOL-UID5) with
// identity hash: LabelPrefix instead of kvo-, the rest of the name cut from its end to fit, then
// -HASH[:8]. At most 32 characters, letters, digits, - and _ (Linode's rule for volume labels).
func (p *Provider) volumeLabel(name, hash string) string {
	prefix := p.LabelPrefix
	if prefix == "" {
		prefix = "kvo-"
	}
	rest := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, strings.TrimPrefix(name, "kvo-"))
	room := maxLabel - len(prefix) - 1 - labelHash
	if room < 0 {
		room = 0
	}
	if len(rest) > room {
		rest = rest[:room]
	}
	l := strings.Trim(prefix+strings.Trim(rest, "-_"), "-_") + "-" + hash[:labelHash]
	for strings.Contains(l, "--") {
		l = strings.ReplaceAll(l, "--", "-")
	}
	if len(l) > maxLabel { // a LabelPrefix longer than a label (refused by New's caller's config check)
		l = l[len(l)-maxLabel:]
	}
	return strings.TrimLeft(l, "-_")
}

// LabelFor is the Linode label EnsureVolume gives the volume v (core's ID and name): for finding
// it in the account, and for tests.
func (p *Provider) LabelFor(v kplugin.Volume) string { return p.volumeLabel(v.Name, identity(v.ID)) }

// handle is what core keeps for a volume: LINODEID:HASH[:24].
func handle(id int, hash string) string { return strconv.Itoa(id) + ":" + hash[:tagHash] }

// parseHandle reads a handle: the Linode volume's ID and the identity tag it must carry. A handle
// of another shape (a bare number, say) names nothing this provider made, and is refused.
func parseHandle(h string) (int, string, error) {
	num, hash, ok := strings.Cut(h, ":")
	id, err := strconv.Atoi(num)
	if !ok || err != nil || len(hash) != tagHash || strings.Trim(hash, "0123456789abcdef") != "" {
		return 0, "", fmt.Errorf("volume handle %q: not one this provider made (want LINODEID:IDENTITY)", h)
	}
	return id, identityTagPrefix + hash, nil
}

// ErrNotThisVolume: a Linode volume doesn't carry the identity tag of the volume it was asked
// about (made outside Kevaso, by another cluster, or for another volume): nothing is done to it.
var ErrNotThisVolume = errors.New("not this volume's: it lacks the identity tag set when it was made")

// owned fetches the volume a handle names and checks it carries the handle's identity tag.
func (p *Provider) owned(ctx context.Context, h string) (*linodego.Volume, int, error) {
	id, tag, err := parseHandle(h)
	if err != nil {
		return nil, 0, err
	}
	v, err := p.API.GetVolume(ctx, id)
	if err != nil {
		return nil, id, apiErr("volume "+strconv.Itoa(id), err)
	}
	if !slices.Contains(v.Tags, tag) {
		return nil, id, fmt.Errorf("volume %d (%s) in Linode, handle %s: %w; it is left as it is", id, v.Label, h, ErrNotThisVolume)
	}
	return v, id, nil
}

// EnsureVolume implements kplugin.Volumes: the volume of identity v.ID, made if it isn't there.
// A volume already labelled as this one would be is adopted only if it carries this identity's
// tag; otherwise the label is taken by something else, and that is refused (never shared).
func (p *Provider) EnsureVolume(ctx context.Context, v kplugin.Volume) (kplugin.VolumeStatus, error) {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	if v.Class != ClassBlock {
		return kplugin.VolumeStatus{}, fmt.Errorf("storage class %s: %w", v.Class, kplugin.ErrUnsupported)
	}
	if v.Region == "" {
		return kplugin.VolumeStatus{}, fmt.Errorf("volume %s: no region", v.Name)
	}
	if v.ID == "" {
		return kplugin.VolumeStatus{}, fmt.Errorf("volume %s: no ID (core sends the silo's UID and the volume's name)", v.Name)
	}
	hash := identity(v.ID)
	label, tag := p.volumeLabel(v.Name, hash), identityTag(hash)
	found, err := p.API.ListVolumes(ctx, filter(map[string]any{"label": label}))
	if err != nil {
		return kplugin.VolumeStatus{}, apiErr("list volumes", err)
	}
	for _, x := range found {
		if x.Label != label {
			continue
		}
		if !slices.Contains(x.Tags, tag) {
			return kplugin.VolumeStatus{}, fmt.Errorf("volume %s: Linode volume %d already has its label %s but not its identity tag %s (another volume's, or made outside Kevaso): not adopting it: %w",
				v.Name, x.ID, label, tag, kplugin.ErrConflict)
		}
		if x.Region != v.Region {
			return kplugin.VolumeStatus{}, fmt.Errorf("volume %s exists in %s, not %s", v.Name, x.Region, v.Region)
		}
		return kplugin.VolumeStatus{Handle: handle(x.ID, hash), Region: x.Region, SizeBytes: int64(x.Size) << 30}, nil
	}
	x, err := p.API.CreateVolume(ctx, linodego.VolumeCreateOptions{Label: label, Region: v.Region, Size: gib(v.SizeBytes), Tags: append(slices.Clone(p.Tags), tag)})
	if err != nil {
		return kplugin.VolumeStatus{}, apiErr("create volume "+label, err)
	}
	return kplugin.VolumeStatus{Handle: handle(x.ID, hash), Region: x.Region, SizeBytes: int64(x.Size) << 30}, nil
}

// waitVolume polls the volume until done says so, for at most d.
func (p *Provider) waitVolume(ctx context.Context, id int, d time.Duration, done func(*linodego.Volume) bool) (*linodego.Volume, bool, error) {
	deadline := time.Now().Add(d)
	for {
		v, err := p.API.GetVolume(ctx, id)
		if err != nil {
			return nil, false, apiErr("volume "+strconv.Itoa(id), err)
		}
		if done(v) {
			return v, true, nil
		}
		if time.Now().After(deadline) {
			return v, false, nil
		}
		select {
		case <-ctx.Done():
			return v, false, ctx.Err()
		case <-time.After(p.dur(p.Poll, 2*time.Second)):
		}
	}
}

func attachedTo(v *linodego.Volume) int {
	if v.LinodeID == nil {
		return 0
	}
	return *v.LinodeID
}

// AttachVolume implements kplugin.Volumes. Attached to another Linode: kplugin.ErrConflict (the
// control plane detaches it from there first, once that host has let go).
func (p *Provider) AttachVolume(ctx context.Context, a kplugin.Attachment) (kplugin.AttachmentStatus, error) {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	lid, err := linodeID(a.Host)
	if err != nil {
		return kplugin.AttachmentStatus{}, err
	}
	v, id, err := p.owned(ctx, a.Handle)
	if err != nil {
		return kplugin.AttachmentStatus{}, err
	}
	switch at := attachedTo(v); {
	case at == lid && v.Status == linodego.VolumeActive:
		return kplugin.AttachmentStatus{Device: v.FilesystemPath}, nil
	case at != 0 && at != lid:
		return kplugin.AttachmentStatus{}, fmt.Errorf("volume %s is attached to Linode %d: %w", v.Label, at, kplugin.ErrConflict)
	case v.Status != linodego.VolumeActive && at == 0:
		return kplugin.AttachmentStatus{}, fmt.Errorf("volume %s is %s: %w", v.Label, v.Status, kplugin.ErrInProgress)
	case at == 0:
		persist := true
		if _, err := p.API.AttachVolume(ctx, id, &linodego.VolumeAttachOptions{LinodeID: lid, PersistAcrossBoots: &persist}); err != nil {
			return kplugin.AttachmentStatus{}, apiErr("attach volume "+v.Label, err)
		}
	}
	v, done, err := p.waitVolume(ctx, id, p.dur(p.AttachWait, 20*time.Second), func(v *linodego.Volume) bool {
		return attachedTo(v) == lid && v.Status == linodego.VolumeActive
	})
	if err != nil {
		return kplugin.AttachmentStatus{}, err
	}
	if !done {
		return kplugin.AttachmentStatus{}, fmt.Errorf("volume %s still attaching: %w", v.Label, kplugin.ErrInProgress)
	}
	return kplugin.AttachmentStatus{Device: v.FilesystemPath}, nil
}

// DetachVolume implements kplugin.Volumes: asks for the detach from a.Host (again, if an earlier
// ask is older than DetachRetry), waits up to DetachWait, and says kplugin.ErrInProgress if it's
// not done. A volume attached to another Linode is not on a.Host: nothing is detached (audit K-037:
// a detach only ever takes a volume off the host it names).
func (p *Provider) DetachVolume(ctx context.Context, a kplugin.Attachment) error {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	lid, err := linodeID(a.Host)
	if err != nil {
		return err
	}
	v, id, err := p.owned(ctx, a.Handle)
	if errors.Is(err, kplugin.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if at := attachedTo(v); at == 0 || at != lid {
		p.forgetDetach(id)
		return nil
	}
	p.mu.Lock()
	if p.detaching == nil {
		p.detaching = map[int]time.Time{}
	}
	last, asked := p.detaching[id]
	again := !asked || time.Since(last) > p.dur(p.DetachRetry, 2*time.Minute)
	if again {
		p.detaching[id] = time.Now()
	}
	p.mu.Unlock()
	if again {
		if err := p.API.DetachVolume(ctx, id); err != nil && !isNotFound(err) {
			return apiErr("detach volume "+v.Label, err)
		}
	}
	_, done, err := p.waitVolume(ctx, id, p.dur(p.DetachWait, 20*time.Second), func(v *linodego.Volume) bool { return attachedTo(v) != lid })
	if err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("volume %s still detaching: %w", v.Label, kplugin.ErrInProgress)
	}
	p.forgetDetach(id)
	return nil
}

func (p *Provider) forgetDetach(id int) {
	p.mu.Lock()
	delete(p.detaching, id)
	p.mu.Unlock()
}

// DeleteVolume implements kplugin.Volumes: only a Linode volume that carries the handle's
// identity tag is deleted; one that doesn't is refused, and left as it is.
func (p *Provider) DeleteVolume(ctx context.Context, ref kplugin.VolumeRef) error {
	ctx, cancel := p.bound(ctx)
	defer cancel()
	v, id, err := p.owned(ctx, ref.Handle)
	if err != nil {
		return err
	}
	if at := attachedTo(v); at != 0 {
		return fmt.Errorf("volume %s is still attached to Linode %d", v.Label, at)
	}
	if err := p.API.DeleteVolume(ctx, id); err != nil {
		return apiErr("delete volume "+v.Label, err)
	}
	return nil
}
