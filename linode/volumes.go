package linode

import (
	"context"
	"fmt"
	"strconv"
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

func volumeID(handle string) (int, error) {
	id, err := strconv.Atoi(handle)
	if err != nil {
		return 0, fmt.Errorf("volume handle %q: %w", handle, err)
	}
	return id, nil
}

// EnsureVolume implements kplugin.Volumes: the volume labelled v.Name, made if it isn't there.
func (p *Provider) EnsureVolume(ctx context.Context, v kplugin.Volume) (kplugin.VolumeStatus, error) {
	if v.Class != ClassBlock {
		return kplugin.VolumeStatus{}, fmt.Errorf("storage class %s: %w", v.Class, kplugin.ErrUnsupported)
	}
	if v.Region == "" {
		return kplugin.VolumeStatus{}, fmt.Errorf("volume %s: no region", v.Name)
	}
	found, err := p.API.ListVolumes(ctx, filter(map[string]any{"label": v.Name}))
	if err != nil {
		return kplugin.VolumeStatus{}, apiErr("list volumes", err)
	}
	for _, x := range found {
		if x.Label == v.Name {
			if x.Region != v.Region {
				return kplugin.VolumeStatus{}, fmt.Errorf("volume %s exists in %s, not %s", v.Name, x.Region, v.Region)
			}
			return kplugin.VolumeStatus{Handle: strconv.Itoa(x.ID), Region: x.Region, SizeBytes: int64(x.Size) << 30}, nil
		}
	}
	x, err := p.API.CreateVolume(ctx, linodego.VolumeCreateOptions{Label: v.Name, Region: v.Region, Size: gib(v.SizeBytes), Tags: p.Tags})
	if err != nil {
		return kplugin.VolumeStatus{}, apiErr("create volume "+v.Name, err)
	}
	return kplugin.VolumeStatus{Handle: strconv.Itoa(x.ID), Region: x.Region, SizeBytes: int64(x.Size) << 30}, nil
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
	id, err := volumeID(a.Handle)
	if err != nil {
		return kplugin.AttachmentStatus{}, err
	}
	lid, err := linodeID(a.Host)
	if err != nil {
		return kplugin.AttachmentStatus{}, err
	}
	v, err := p.API.GetVolume(ctx, id)
	if err != nil {
		return kplugin.AttachmentStatus{}, apiErr("volume "+a.Handle, err)
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

// DetachVolume implements kplugin.Volumes: asks for the detach (again, if an earlier ask is older
// than DetachRetry), waits up to DetachWait, and says kplugin.ErrInProgress if it's not done.
func (p *Provider) DetachVolume(ctx context.Context, a kplugin.Attachment) error {
	id, err := volumeID(a.Handle)
	if err != nil {
		return err
	}
	v, err := p.API.GetVolume(ctx, id)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return apiErr("volume "+a.Handle, err)
	}
	if attachedTo(v) == 0 {
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
	_, done, err := p.waitVolume(ctx, id, p.dur(p.DetachWait, 20*time.Second), func(v *linodego.Volume) bool { return attachedTo(v) == 0 })
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

// DeleteVolume implements kplugin.Volumes.
func (p *Provider) DeleteVolume(ctx context.Context, ref kplugin.VolumeRef) error {
	id, err := volumeID(ref.Handle)
	if err != nil {
		return err
	}
	v, err := p.API.GetVolume(ctx, id)
	if err != nil {
		return apiErr("volume "+ref.Handle, err)
	}
	if at := attachedTo(v); at != 0 {
		return fmt.Errorf("volume %s is still attached to Linode %d", v.Label, at)
	}
	if err := p.API.DeleteVolume(ctx, id); err != nil {
		return apiErr("delete volume "+v.Label, err)
	}
	return nil
}
