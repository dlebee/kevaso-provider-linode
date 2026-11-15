package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The package's manifest (kvoadm plugin install --manifest) declares where its Block Storage
// volumes' devices are, the only place kevasod accepts its AttachVolume answers (core's K-038):
// Linode's by-id name for a volume, which is the filesystem_path the API gives and the provider
// answers. Each side asks for nothing beyond the default.
func TestManifestDeclaresTheVolumesDevices(t *testing.T) {
	b, err := os.ReadFile("../../kevaso-plugin.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Name           string          `json:"name"`
		Controller     json.RawMessage `json:"controller"`
		Node           json.RawMessage `json:"node"`
		DevicePrefixes []string        `json:"devicePrefixes"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Name != "linode" || m.Controller != nil || m.Node != nil {
		t.Fatalf("manifest: %+v", m)
	}
	const dev = "/dev/disk/by-id/scsi-0Linode_Volume_kvo-shop-db-data-0f3a2" // the API's filesystem_path
	if len(m.DevicePrefixes) != 1 || !strings.HasPrefix(dev, m.DevicePrefixes[0]) {
		t.Fatalf("device prefixes %v don't hold %s", m.DevicePrefixes, dev)
	}
}
