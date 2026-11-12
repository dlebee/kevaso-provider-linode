// Package node is the provider's node side, on each Linode worker: it announces the shared IPv4s
// the host's silos are reached on over BGP to Linode's route servers, the way Linode's lelastic
// does (research E031: same peers, ASNs, timers and community; lelastic is Apache-2.0). Unlike
// lelastic, which reads the loopback once at start, it follows the loopback: kevasod puts a
// silo's shared address there while the silo is on this host, and takes it away when it leaves,
// and the announcement follows within seconds. It holds no token and needs no privileges.
package node

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"time"

	api "github.com/osrg/gobgp/v3/api"
	"github.com/osrg/gobgp/v3/pkg/server"
	"google.golang.org/protobuf/types/known/anypb"
)

// BGP settings, as lelastic has them.
const (
	localASN       = 65001
	routeServerASN = 65000
	routerID       = "10.0.0.1"
	// CommunityPrimary marks this host's announcements as the primary route.
	communityPrimary = 65000<<16 | 1
)

// DCIDs are Linode's data-center IDs for BGP (Akamai's "Configure failover" page, November 2026):
// the route servers are 2600:3c0f:DCID:34::1-4.
var DCIDs = map[string]int{
	"nl-ams": 22, "us-southeast": 4, "in-maa": 25, "us-ord": 18, "us-central": 2, "eu-central": 10,
	"de-fra-2": 47, "us-west": 3, "id-cgk": 29, "eu-west": 7, "gb-lon": 44, "us-lax": 30, "es-mad": 24,
	"au-mel": 45, "us-mia": 28, "it-mil": 27, "ap-west": 14, "in-bom-2": 46, "us-east": 6, "jp-osa": 26,
	"fr-par": 19, "fr-par-2": 53, "br-gru": 21, "us-sea": 20, "ap-south": 9, "sg-sin-2": 48, "se-sto": 23,
	"ap-southeast": 16, "ap-northeast": 11, "jp-tyo-3": 49, "ca-central": 15, "us-iad": 17,
}

// Announcer announces the loopback's shared addresses.
type Announcer struct {
	DCID  int
	Every time.Duration // how often to look at the loopback (default 2s)
	Log   func(string, ...any)
	// Addresses lists what to announce (default: the loopback's global IPv4 /32s).
	Addresses func() ([]netip.Addr, error)
}

// LoopbackAddresses are lo's global IPv4 /32s (not 127/8).
func LoopbackAddresses() ([]netip.Addr, error) {
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		return nil, err
	}
	addrs, err := lo.Addrs()
	if err != nil {
		return nil, err
	}
	var out []netip.Addr
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP.To4())
		if ones, bits := n.Mask.Size(); !ok || ones != 32 || bits != 32 || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ip)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out, nil
}

func (a *Announcer) log(f string, args ...any) {
	if a.Log != nil {
		a.Log(f, args...)
	}
}

func path(ip netip.Addr) (*api.Path, error) {
	nlri, err := anypb.New(&api.IPAddressPrefix{Prefix: ip.String(), PrefixLen: 32})
	if err != nil {
		return nil, err
	}
	family := &api.Family{Afi: api.Family_AFI_IP, Safi: api.Family_SAFI_UNICAST}
	origin, _ := anypb.New(&api.OriginAttribute{Origin: 2})
	nh, _ := anypb.New(&api.MpReachNLRIAttribute{Family: family, Nlris: []*anypb.Any{nlri}})
	com, _ := anypb.New(&api.CommunitiesAttribute{Communities: []uint32{communityPrimary}})
	return &api.Path{Family: family, Nlri: nlri, Pattrs: []*anypb.Any{origin, nh, com}}, nil
}

// Run announces until ctx ends; on return, every route is withdrawn (the BGP sessions close).
func (a *Announcer) Run(ctx context.Context) error {
	if a.DCID <= 1 {
		return fmt.Errorf("no data-center ID for BGP")
	}
	list := a.Addresses
	if list == nil {
		list = LoopbackAddresses
	}
	s := server.NewBgpServer()
	go s.Serve()
	defer s.Stop()
	if err := s.StartBgp(ctx, &api.StartBgpRequest{Global: &api.Global{Asn: localASN, RouterId: routerID, ListenPort: -1}}); err != nil {
		return err
	}
	if err := s.WatchEvent(ctx, &api.WatchEventRequest{Peer: &api.WatchEventRequest_Peer{}}, func(r *api.WatchEventResponse) {
		if p := r.GetPeer(); p != nil && p.Type == api.WatchEventResponse_PeerEvent_STATE {
			a.log("route server %s: %s", p.Peer.Conf.NeighborAddress, p.Peer.State.SessionState)
		}
	}); err != nil {
		return err
	}
	for i := 1; i <= 4; i++ {
		rs := fmt.Sprintf("2600:3c0f:%d:34::%d", a.DCID, i)
		if err := s.AddPeer(ctx, &api.AddPeerRequest{Peer: &api.Peer{
			ApplyPolicy: &api.ApplyPolicy{
				ExportPolicy: &api.PolicyAssignment{Name: "rs-out", Direction: api.PolicyDirection_EXPORT, DefaultAction: api.RouteAction_ACCEPT},
				ImportPolicy: &api.PolicyAssignment{Name: "rs-in", Direction: api.PolicyDirection_IMPORT, DefaultAction: api.RouteAction_REJECT},
			},
			Conf:         &api.PeerConf{NeighborAddress: rs, PeerAsn: routeServerASN, Description: "Linode route server"},
			EbgpMultihop: &api.EbgpMultihop{Enabled: true, MultihopTtl: 16},
			Timers:       &api.Timers{Config: &api.TimersConfig{ConnectRetry: 5, HoldTime: 9, KeepaliveInterval: 3}},
			AfiSafis: []*api.AfiSafi{
				{Config: &api.AfiSafiConfig{Family: &api.Family{Afi: api.Family_AFI_IP, Safi: api.Family_SAFI_UNICAST}, Enabled: true}},
				{Config: &api.AfiSafiConfig{Family: &api.Family{Afi: api.Family_AFI_IP6, Safi: api.Family_SAFI_UNICAST}, Enabled: true}},
			},
		}}); err != nil {
			return fmt.Errorf("route server %s: %w", rs, err)
		}
	}
	every := a.Every
	if every <= 0 {
		every = 2 * time.Second
	}
	announced := map[netip.Addr]*api.Path{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		want, err := list()
		if err != nil {
			a.log("loopback: %v", err)
		}
		wanted := map[netip.Addr]bool{}
		for _, ip := range want {
			wanted[ip] = true
			if announced[ip] != nil {
				continue
			}
			p, err := path(ip)
			if err != nil {
				return err
			}
			if _, err := s.AddPath(ctx, &api.AddPathRequest{Path: p}); err != nil {
				a.log("announce %s: %v", ip, err)
				continue
			}
			announced[ip] = p
			a.log("announcing %s (primary)", ip)
		}
		for ip, p := range announced {
			if wanted[ip] {
				continue
			}
			if err := s.DeletePath(ctx, &api.DeletePathRequest{Path: p}); err != nil {
				a.log("withdraw %s: %v", ip, err)
				continue
			}
			delete(announced, ip)
			a.log("withdrew %s", ip)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
