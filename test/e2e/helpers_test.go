//go:build e2e

package e2e

import (
	"os"
	"testing"

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
