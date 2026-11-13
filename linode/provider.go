// Package linode is Kevaso's Linode provider (design §12): the controller side, which holds the
// Linode API token and serves, over the plugin protocol (kevaso.com/core/sdk/kplugin):
//
//   - hosts: which cluster hosts are Linodes (by address), their region and private address;
//   - load-balancer class linode-nodebalancer: a NodeBalancer per silo, sending each port to the
//     silo's host on a node port, over the region's private network;
//   - load-balancer class linode-shared-ip: an IPv4 you own, shared with every Linode host in its
//     region and announced over BGP by whichever host has the silo (research E031);
//   - storage class linode-block: Block Storage volumes, attached to the silo's host; a detach can
//     hang, so it's started, polled for a while, and asked again later (E032);
//   - DNS provider linode: records in Linode DNS Manager, for Ingress host names and DNS-01.
//
// Everything it creates is tagged (kevaso, plus the tags in its config), so it can be found.
package linode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linode/linodego"

	"kevaso.com/core/sdk/kplugin"
)

// Version of this provider.
var Version = "0.1.0"

// Class names.
const (
	ClassNodeBalancer = "linode-nodebalancer"
	ClassSharedIP     = "linode-shared-ip"
	ClassBlock        = "linode-block"
	DNSName           = "linode"
)

// NodeBalancerSources is where NodeBalancers connect to backends from (Linode's documented range).
const NodeBalancerSources = "192.168.255.0/24"

// Config is the provider's settings (not secret; the token comes separately).
type Config struct {
	// Tags go on everything it creates, besides "kevaso".
	Tags []string `json:"tags,omitempty"`
	// LabelPrefix, if set, replaces the "kvo-" that starts the labels of the volumes and
	// NodeBalancers it creates, for telling apart what several clusters made in one account.
	LabelPrefix string `json:"labelPrefix,omitempty"`
}

// Provider is the Linode provider.
type Provider struct {
	API  *linodego.Client
	Tags []string // on everything created ("kevaso" first)
	// LabelPrefix replaces "kvo-" at the start of the labels it gives (Config.LabelPrefix).
	LabelPrefix string
	// AttachWait and DetachWait are how long one call waits for an attach or detach to finish
	// before answering kplugin.ErrInProgress (default 20s each). DetachRetry is how long after
	// asking for a detach it's asked again (one detach hung for over 5 minutes in E032; a second
	// request went through in 20 s). Poll is how often it looks meanwhile.
	AttachWait, DetachWait, DetachRetry, Poll time.Duration

	mu        sync.Mutex
	detaching map[int]time.Time // volume → when its detach was last asked for
	instances []linodego.Instance
	listed    time.Time
}

// New returns a provider using token.
func New(token string, cfg Config) *Provider {
	c := linodego.NewClient(&http.Client{Timeout: 60 * time.Second})
	c.SetToken(token)
	c.SetUserAgent("kevaso-provider-linode/" + Version)
	c.SetRetries()
	return &Provider{API: &c, Tags: append([]string{"kevaso"}, cfg.Tags...), LabelPrefix: cfg.LabelPrefix}
}

// maxLabel is the longest label Linode takes on a volume or a NodeBalancer.
const maxLabel = 32

// label is what a volume or NodeBalancer named name (from core: "kvo-…", unique by its end) is
// labelled in Linode: with LabelPrefix instead of "kvo-", cut from the middle to fit so the
// unique end stays.
func (p *Provider) label(name string) string {
	if p.LabelPrefix == "" {
		return name
	}
	rest := strings.TrimPrefix(name, "kvo-")
	if room := maxLabel - len(p.LabelPrefix); len(rest) > room {
		rest = rest[len(rest)-max(room, 0):]
	}
	return strings.ReplaceAll(strings.ReplaceAll(p.LabelPrefix+strings.TrimLeft(rest, "-_"), "--", "-"), "__", "_")
}

func (p *Provider) dur(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Info implements kplugin.Plugin.
func (p *Provider) Info(context.Context) (kplugin.Info, error) {
	return kplugin.Info{Name: "linode", Version: Version, Protocol: kplugin.Protocol, Hosts: true,
		LoadBalancers: []kplugin.Class{
			{Name: ClassNodeBalancer, Description: "a Linode NodeBalancer, to the silo's host over the private network", OwnHostsOnly: true, NodePorts: true},
			{Name: ClassSharedIP, Description: "an IPv4 you own, shared between the region's Linodes, announced over BGP by the silo's host", OwnHostsOnly: true, Announce: true},
		},
		// Regional: a volume detaches and attaches to any Linode in its region, so a silo on it
		// can move within the region (design §0.3).
		Storage: []kplugin.Class{{Name: ClassBlock, Description: "Linode Block Storage, attached to the silo's host", OwnHostsOnly: true,
			Topology: kplugin.TopologyRegion}},
		DNS:     []string{DNSName}}, nil
}

// instancesNow lists the account's Linodes (cached for 30 s: describing many hosts is one call).
func (p *Provider) instancesNow(ctx context.Context) ([]linodego.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.instances != nil && time.Since(p.listed) < 30*time.Second {
		return p.instances, nil
	}
	is, err := p.API.ListInstances(ctx, nil)
	if err != nil {
		return nil, apiErr("list Linodes", err)
	}
	p.instances, p.listed = is, time.Now()
	return is, nil
}

var privateNet = netip.MustParsePrefix("192.168.128.0/17") // Linode private IPv4s

// DescribeHosts implements kplugin.HostDescriber: a host is a Linode when one of its IPv4s is the
// host's address.
func (p *Provider) DescribeHosts(ctx context.Context, hosts []kplugin.Host) ([]kplugin.HostInfo, error) {
	is, err := p.instancesNow(ctx)
	if err != nil {
		return nil, err
	}
	var out []kplugin.HostInfo
	for _, h := range hosts {
		want, err := netip.ParseAddr(h.Address)
		if err != nil {
			continue
		}
		for _, in := range is {
			match, private := false, ""
			for _, ip := range in.IPv4 {
				a, ok := netip.AddrFromSlice(ip.To4())
				if !ok {
					continue
				}
				if a == want {
					match = true
				}
				if privateNet.Contains(a) {
					private = a.String()
				}
			}
			if match {
				out = append(out, kplugin.HostInfo{Name: h.Name, ID: strconv.Itoa(in.ID), Region: in.Region, PrivateAddress: private})
			}
		}
	}
	return out, nil
}

func linodeID(h kplugin.HostInfo) (int, error) {
	id, err := strconv.Atoi(h.ID)
	if err != nil {
		return 0, fmt.Errorf("host %s: Linode ID %q: %w", h.Name, h.ID, err)
	}
	return id, nil
}

// apiErr says what failed, and maps a 404 to kplugin.ErrNotFound.
func apiErr(what string, err error) error {
	if err == nil {
		return nil
	}
	var le *linodego.Error
	if errors.As(err, &le) && le.Code == http.StatusNotFound {
		return fmt.Errorf("%s: %w", what, kplugin.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}

func isNotFound(err error) bool {
	var le *linodego.Error
	return errors.As(err, &le) && le.Code == http.StatusNotFound
}

func filter(kv map[string]any) *linodego.ListOptions {
	f := linodego.Filter{}
	for k, v := range kv {
		f.AddField(linodego.Eq, k, v)
	}
	s, _ := f.MarshalJSON()
	return linodego.NewListOptions(0, string(s))
}

var (
	_ kplugin.Plugin        = (*Provider)(nil)
	_ kplugin.HostDescriber = (*Provider)(nil)
	_ kplugin.LoadBalancers = (*Provider)(nil)
	_ kplugin.Volumes       = (*Provider)(nil)
	_ kplugin.DNS           = (*Provider)(nil)
)
