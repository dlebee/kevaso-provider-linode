//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kevaso.com/provider-linode/linodetest"
)

// The tests' names for linodetest's helpers (one set, shared with other providers' e2e tests).
type (
	bed      = linodetest.Bed
	machine  = linodetest.Machine
	kit      = linodetest.Kit
	cluster  = linodetest.Cluster
	hostView = linodetest.HostView
)

var (
	newBed     = linodetest.NewBed
	newKit     = linodetest.NewKit
	lookup     = linodetest.Lookup
	eventually = linodetest.Eventually
)

func decodeList[T any](out string) ([]T, error) { return linodetest.DecodeList[T](out) }

// TestSweep deletes what runs of a family that died left behind (linodetest.SweepFamily):
//
//	KEVASO_E2E_SWEEP=kvt-ha- KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -run TestSweep ./test/e2e/
func TestSweep(t *testing.T) {
	prefix, tokenFile := os.Getenv("KEVASO_E2E_SWEEP"), os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if prefix == "" || tokenFile == "" {
		t.Skip("KEVASO_E2E_SWEEP and KEVASO_LINODE_TOKEN_FILE not set")
	}
	linodetest.SweepFamily(t, prefix, tokenFile)
}

// TestLeftovers lists (deletes nothing) every kvt- or kevaso-e2e- resource in the account, of any
// run, and the IPv6 ranges and additional IPv4s that aren't on the account's other Linodes: what
// each turn ends with, to show the account is clean.
//
//	KEVASO_LINODE_TOKEN_FILE=… go test -tags e2e -run TestLeftovers -v ./test/e2e/
func TestLeftovers(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set")
	}
	b := newBed(t, tokenFile, "kvt-list-")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	left, reserved, err := b.Leftovers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("instances, volumes, NodeBalancers, domains, firewalls, VPCs labelled kvt-/kevaso-e2e-: %d %v", len(left), left)
	t.Logf("reserved IPv4 addresses in the account: %d", reserved)
	extras, others, err := b.Extras(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("IPv6 ranges and additional IPv4s on kvt-/kevaso-e2e- Linodes or on none: %d %v (on the account's other Linodes, untouched: %d)", len(extras), extras, others)
	if left = append(left, extras...); len(left) > 0 {
		t.Errorf("left in the account: %v", left)
	}
}

// manifestFile is the plugin's manifest (kvoadm plugin install --manifest): where its volumes'
// devices are, which kevasod checks its answers against (core's K-038).
func manifestFile(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", "..", "kevaso-plugin.json"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
