package linode

import (
	"errors"
	"sort"
	"strings"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/provider-linode/dcid"
)

// Annotation keys a LoadBalancer service takes, declared without the domain: core puts the install
// name's in front (linode.kevaso.com/nodebalancer; a second account installed as linode-work reads
// linode-work.kevaso.com/nodebalancer).
const (
	// KeyNodeBalancer shares one NodeBalancer, by name, between the services of a namespace that
	// name it (each on its own ports). Without it a service gets its own.
	KeyNodeBalancer = "nodebalancer"
	// KeyNodeBalancerType is the NodeBalancer's plan (Linode has no sizes; it has types): common
	// (the default) or premium. Fixed when the NodeBalancer is made.
	KeyNodeBalancerType = "nodebalancer-type"
	// KeyClientConnThrottle limits new connections per second per client IP (0-20, 0 = off).
	KeyClientConnThrottle = "client-conn-throttle"
	// KeyAlgorithm is how a port picks a backend: roundrobin (default), leastconn or source.
	KeyAlgorithm = "algorithm"
	// KeyProxyProtocol sends the client's address to the silo with PROXY protocol: none (default),
	// v1 or v2. The only way the silo sees the client's address behind a NodeBalancer (it proxies
	// TCP); the silo must speak PROXY protocol on that port.
	KeyProxyProtocol = "proxy-protocol"
	// KeyCheck is the backends' health check: connection (default) or none.
	KeyCheck = "check"
	// KeyCheckInterval, KeyCheckTimeout and KeyCheckAttempts tune it (seconds, seconds, tries).
	KeyCheckInterval = "check-interval"
	KeyCheckTimeout  = "check-timeout"
	KeyCheckAttempts = "check-attempts"
)

// Shared NodeBalancer names: its label is kvo-NAME-HASH (32 characters at most).
const sharedNamePattern = `[a-z0-9]([a-z0-9-]{0,17}[a-z0-9])?`

func i64(n int64) *int64 { return &n }

// Networking is what the provider declares (Info.Networking): Provider IPs are Linode shared IPv4s
// (reserved in a region, announced over BGP by the silo's Linode, so only in regions with BGP
// failover); load balancers are NodeBalancers, which have their own address (no kept IP in front)
// and keep the client's address only with PROXY protocol. Both reach the silo within one region,
// on the account's Linodes only.
func Networking() *kplugin.Networking {
	svc := []string{kplugin.OnService}
	return &kplugin.Networking{
		ProviderIPs: true, LoadBalancers: true, FrontWithIP: false, KeepsClientIP: true,
		Reach: kplugin.FollowsRegion, Regions: dcid.Regions(), OwnHostsOnly: true,
		Annotations: []kplugin.Annotation{
			{Key: KeyNodeBalancer, Description: "share one NodeBalancer by name with the namespace's other services naming it (each on its own ports); default: one per service", Pattern: sharedNamePattern, On: svc},
			{Key: KeyNodeBalancerType, Description: "the NodeBalancer's plan: common (default) or premium; fixed when it is made", Type: kplugin.AnnotationEnum, Values: []string{"common", "premium"}, On: svc},
			{Key: KeyClientConnThrottle, Description: "new connections per second per client IP (0 = no limit)", Type: kplugin.AnnotationInt, Min: i64(0), Max: i64(20), On: svc},
			{Key: KeyAlgorithm, Description: "how a port picks a backend", Type: kplugin.AnnotationEnum, Values: []string{"roundrobin", "leastconn", "source"}, On: svc},
			{Key: KeyProxyProtocol, Description: "send the client's address with PROXY protocol (the silo must speak it): the only way it sees the client's address", Type: kplugin.AnnotationEnum, Values: []string{"none", "v1", "v2"}, On: svc},
			{Key: KeyCheck, Description: "the backends' health check", Type: kplugin.AnnotationEnum, Values: []string{"connection", "none"}, On: svc},
			{Key: KeyCheckInterval, Description: "seconds between health checks", Type: kplugin.AnnotationInt, Min: i64(2), Max: i64(3600), On: svc},
			{Key: KeyCheckTimeout, Description: "seconds a health check waits", Type: kplugin.AnnotationInt, Min: i64(1), Max: i64(30), On: svc},
			{Key: KeyCheckAttempts, Description: "failed checks before a backend is taken out", Type: kplugin.AnnotationInt, Min: i64(1), Max: i64(30), On: svc},
		},
	}
}

// domainOf is the NAME.kevaso.com/ an annotation name starts with, or "".
func domainOf(k string) string {
	i := strings.Index(k, ".kevaso.com/")
	if i <= 0 {
		return ""
	}
	return k[:i+len(".kevaso.com/")]
}

// settings are the annotations core sent (already in the plugin's domain, whichever name it is
// installed under), by key, checked against the declaration for on. Annotations outside a
// kevaso.com domain are ignored.
func settings(on string, ms ...map[string]string) (map[string]string, error) {
	out := map[string]string{}
	var errs []error
	n := Networking()
	for _, m := range ms {
		byDomain := map[string]map[string]string{}
		for k, v := range m {
			if d := domainOf(k); d != "" {
				if byDomain[d] == nil {
					byDomain[d] = map[string]string{}
				}
				byDomain[d][k] = v
			}
		}
		domains := make([]string, 0, len(byDomain))
		for d := range byDomain {
			domains = append(domains, d)
		}
		sort.Strings(domains)
		for _, d := range domains {
			if err := n.ValidateAnnotations(strings.TrimSuffix(d, ".kevaso.com/"), on, byDomain[d]); err != nil {
				errs = append(errs, err)
				continue
			}
			for k, v := range byDomain[d] {
				out[strings.TrimPrefix(k, d)] = v
			}
		}
	}
	return out, errors.Join(errs...)
}
