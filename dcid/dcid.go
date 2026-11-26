// Package dcid lists Linode's data-center IDs for BGP: the regions where a shared IPv4 can be
// announced (IP failover), so the regions where the provider makes Provider IPs.
package dcid

import "sort"

// IDs are Linode's data-center IDs for BGP (Akamai's "Configure failover" page, November 2026):
// the route servers are 2600:3c0f:DCID:34::1-4.
var IDs = map[string]int{
	"nl-ams": 22, "us-southeast": 4, "in-maa": 25, "us-ord": 18, "us-central": 2, "eu-central": 10,
	"de-fra-2": 47, "us-west": 3, "id-cgk": 29, "eu-west": 7, "gb-lon": 44, "us-lax": 30, "es-mad": 24,
	"au-mel": 45, "us-mia": 28, "it-mil": 27, "ap-west": 14, "in-bom-2": 46, "us-east": 6, "jp-osa": 26,
	"fr-par": 19, "fr-par-2": 53, "br-gru": 21, "us-sea": 20, "ap-south": 9, "sg-sin-2": 48, "se-sto": 23,
	"ap-southeast": 16, "ap-northeast": 11, "jp-tyo-3": 49, "ca-central": 15, "us-iad": 17,
}

// Regions are the regions with a DCID, sorted.
func Regions() []string {
	out := make([]string, 0, len(IDs))
	for r := range IDs {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}
