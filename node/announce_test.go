package node

import (
	"net/netip"
	"testing"

	api "github.com/osrg/gobgp/v3/api"
)

func TestPath(t *testing.T) {
	p, err := path(netip.MustParseAddr("172.105.3.63"))
	if err != nil {
		t.Fatal(err)
	}
	var pfx api.IPAddressPrefix
	if err := p.Nlri.UnmarshalTo(&pfx); err != nil || pfx.Prefix != "172.105.3.63" || pfx.PrefixLen != 32 {
		t.Fatalf("nlri %+v %v", &pfx, err)
	}
	var com api.CommunitiesAttribute
	if err := p.Pattrs[2].UnmarshalTo(&com); err != nil || len(com.Communities) != 1 || com.Communities[0] != 65000<<16|1 {
		t.Fatalf("community %+v %v (lelastic's primary is 65000:1)", &com, err)
	}
}

func TestDCIDs(t *testing.T) {
	if DCIDs["ca-central"] != 15 { // measured in E031
		t.Fatal("ca-central")
	}
}
