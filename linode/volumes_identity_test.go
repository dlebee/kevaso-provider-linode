package linode

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/core/sdk/kplugin/kplugintest"
)

// recordedVolume is a volume as GET /v4/volumes/ID returns it (the shape recorded from the real
// API, with this test's values): made in Cloud Manager, outside Kevaso, so with no identity tag.
const recordedVolume = `{"id": 0, "label": "LABEL", "status": "active", "size": 20, "region": "ca-central",
 "linode_id": null, "linode_label": null, "filesystem_path": "/dev/disk/by-id/scsi-0Linode_Volume_LABEL",
 "tags": ["prod"], "hardware_type": "nvme", "encryption": "enabled",
 "created": "2026-11-01T10:00:00", "updated": "2026-11-01T10:00:00"}`

func (f *fakeAPI) addRecordedVolume(t *testing.T, label string, linode int) int {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(strings.ReplaceAll(recordedVolume, "LABEL", label)), &v); err != nil {
		t.Fatal(err)
	}
	id := f.id()
	v["id"] = id
	if linode != 0 {
		v["linode_id"] = linode
	}
	f.volumes[id] = v
	return id
}

// K-035: two volumes are never one Linode volume, whatever their names. Core's human name is the
// same for both here (the audit's case: a long namespace and silo name left no room for the
// volume's), their IDs differ: two Linode volumes, two handles, and removing one leaves the other.
func TestTwoVolumesNeverShareOneLinodeVolume(t *testing.T) {
	f, p := newFakeAPI(t)
	name := "kvo-production-payments-db-a1b2c" // what an old core gave both data and wal
	data, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "uid-1/data", Name: name, SizeBytes: 1, Region: "ca-central"})
	if err != nil {
		t.Fatal(err)
	}
	wal, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "uid-1/wal", Name: name, SizeBytes: 1, Region: "ca-central"})
	if err != nil {
		t.Fatal(err)
	}
	if data.Handle == wal.Handle || len(f.volumes) != 2 {
		t.Fatalf("two volumes, one Linode volume: %s %s (%d in the account)", data.Handle, wal.Handle, len(f.volumes))
	}
	labels := map[any]bool{}
	for _, v := range f.volumes {
		labels[v["label"]] = true
		if l := v["label"].(string); len(l) > maxLabel {
			t.Errorf("label %s: %d characters", l, len(l))
		}
	}
	if len(labels) != 2 {
		t.Fatalf("labels: %v", labels)
	}
	if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: wal.Handle}); err != nil {
		t.Fatal(err)
	}
	if again, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "uid-1/data", Name: name, SizeBytes: 1, Region: "ca-central"}); err != nil || again.Handle != data.Handle {
		t.Fatalf("data after wal's delete: %+v %v", again, err)
	}
	if len(f.volumes) != 1 {
		t.Fatalf("%d volumes after deleting one of two", len(f.volumes))
	}
}

// A Linode volume that has the label this volume would get, but not its identity tag (made by
// hand, or by another cluster with the same prefix), is never adopted: EnsureVolume refuses, and
// makes nothing.
func TestALabelTakenByAnotherVolumeIsRefused(t *testing.T) {
	f, p := newFakeAPI(t)
	v := kplugin.Volume{Class: ClassBlock, ID: "uid-2/data", Name: "kvo-shop-db-data-c3d4e", SizeBytes: 1, Region: "ca-central"}
	taken := f.addRecordedVolume(t, p.volumeLabel(v.Name, identity(v.ID)), 0)
	_, err := p.EnsureVolume(ctx, v)
	if !errors.Is(err, kplugin.ErrConflict) || !strings.Contains(err.Error(), "not adopting") {
		t.Fatalf("a taken label: %v", err)
	}
	if len(f.volumes) != 1 || f.volumes[taken]["tags"].([]any)[0] != "prod" {
		t.Fatalf("the account changed: %v", f.volumes)
	}
	t.Logf("refused: %v", err)
}

// Nothing is attached, detached or deleted that doesn't carry the handle's identity tag: a handle
// naming a volume made outside Kevaso (a worker that rewrote its silo's status, K-037; a handle of
// another shape) leaves that volume as it is.
func TestAHandleActsOnlyOnItsOwnVolume(t *testing.T) {
	f, p := newFakeAPI(t)
	tor := kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central"}
	foreign := f.addRecordedVolume(t, "owner-db", 12)
	forged := handle(foreign, identity("uid-3/data"))
	for _, h := range []string{forged, strconv.Itoa(foreign), strconv.Itoa(foreign) + ":zz", ""} {
		if _, err := p.AttachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: h, Host: tor}); err == nil {
			t.Errorf("attach %q went ahead", h)
		}
		if err := p.DetachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: h, Host: kplugin.HostInfo{Name: "x", ID: "12"}}); err == nil {
			t.Errorf("detach %q went ahead", h)
		}
		if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: h}); err == nil {
			t.Errorf("delete %q went ahead", h)
		}
	}
	if err := p.DeleteVolume(ctx, kplugin.VolumeRef{Class: ClassBlock, Handle: forged}); !errors.Is(err, ErrNotThisVolume) {
		t.Errorf("delete of a foreign volume: %v", err)
	}
	if v, ok := f.volumes[foreign]; !ok || v["linode_id"] != 12 {
		t.Fatalf("the foreign volume changed: %v", v)
	}
	for _, c := range f.calls {
		if !strings.HasPrefix(c, "GET ") {
			t.Errorf("a call that changes something: %s", c)
		}
	}
}

// A detach takes a volume off the host it names only: attached to another Linode, it isn't on
// that host, and nothing is asked of the API.
func TestDetachOnlyFromTheHostNamed(t *testing.T) {
	f, p := newFakeAPI(t)
	tor, chi := kplugin.HostInfo{Name: "tor", ID: "11"}, kplugin.HostInfo{Name: "chi", ID: "12"}
	st, err := p.EnsureVolume(ctx, kplugin.Volume{Class: ClassBlock, ID: "uid-4/data", Name: "kvo-a-b-c-12345", SizeBytes: 1, Region: "ca-central"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.AttachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: chi}); err != nil {
		t.Fatal(err)
	}
	if err := p.DetachVolume(ctx, kplugin.Attachment{Class: ClassBlock, Handle: st.Handle, Host: tor}); err != nil {
		t.Fatalf("detach from a host it isn't on: %v", err)
	}
	id, _, _ := parseHandle(st.Handle)
	if f.volumes[id]["linode_id"] != 12 || count(f.calls, "POST /volumes/"+strconv.Itoa(id)+"/detach") != 0 {
		t.Fatalf("detached from chi when asked about tor: %v %v", f.volumes[id], f.calls)
	}
}

// The plugin protocol's promises for linode-block over the wire (core's conformance kit: create,
// grow, attach, let go, delete, each twice), against the fake API (audit K-055: the kit's volume
// section had never run against this provider).
func TestConformanceBlock(t *testing.T) {
	_, p := newFakeAPI(t)
	kplugintest.Conformance(t, kplugintest.Suite{Plugin: p, Region: "ca-central", Classes: []string{ClassBlock},
		Host: kplugin.HostInfo{Name: "tor", ID: "11", Region: "ca-central", PrivateAddress: "192.168.140.7"}})
}
