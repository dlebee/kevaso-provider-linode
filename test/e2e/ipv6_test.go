//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// The IPv6 effort on Linode (wave 3): each worker's own IPv6 (its SLAAC address) is the
// externalIPv6 the worker finds; a HostPort service answers on it from outside with the client's
// address kept; a NodeBalancer answers on its IPv6 too; an IPv6 Provider IP is refused at apply.

// echo6 answers "NAME PORT CLIENT" on every port it is given, over IPv4 and IPv6 (one dual-stack
// socket per port): CLIENT is the address the connection came from as the silo sees it, unmapped.
const echo6 = `import http.server as h, socket, threading as t, sys
class S(h.ThreadingHTTPServer):
    address_family = socket.AF_INET6
    def server_bind(s):
        s.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()
class H(h.BaseHTTPRequestHandler):
    def do_GET(s):
        c = s.client_address[0]
        if c.startswith("::ffff:"): c = c[7:]
        b = ("%s %d %s\n" % (sys.argv[1], s.server.server_port, c)).encode()
        s.send_response(200); s.send_header("Content-Length", str(len(b))); s.end_headers(); s.wfile.write(b)
    def log_message(s, *a): pass
for p in sys.argv[2:]:
    t.Thread(target=S(("::", int(p)), H).serve_forever).start()
`

// echo6Command is echo6 as a silo's container command (YAML flow list).
func echo6Command(name string, port int) string {
	return fmt.Sprintf(`[python3, -c, "exec(__import__('base64').b64decode('%s'))", %s, "%d"]`,
		base64.StdEncoding.EncodeToString([]byte(echo6)), name, port)
}

// fromHere6 GETs http://[addr]:port/ from this machine over IPv6, and says which of its addresses
// it went from (the source the OS picks for that destination).
func fromHere6(addr string, port int) (body, from string, err error) {
	target := net.JoinHostPort(addr, fmt.Sprint(port))
	if c, err := net.Dial("udp6", target); err == nil { // nothing is sent
		from = c.LocalAddr().(*net.UDPAddr).IP.String()
		_ = c.Close()
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	client := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true, DialContext: d.DialContext}}
	resp, err := client.Get("http://" + target + "/")
	if err != nil {
		return "", from, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return strings.TrimSpace(string(b)), from, err
}

// hereHasIPv6 says whether this machine reaches the internet over IPv6 (a route to a global
// address); without it the from-outside IPv6 checks can't run.
func hereHasIPv6() bool {
	c, err := net.Dial("udp6", "[2600::]:80")
	if err != nil {
		return false
	}
	defer func() { _ = c.Close() }()
	a, ok := netip.AddrFromSlice(c.LocalAddr().(*net.UDPAddr).IP)
	return ok && a.IsGlobalUnicast() && !a.IsPrivate()
}

type hostIPv6 struct {
	Spec struct {
		ExternalIPv6 string `json:"externalIPv6"`
	} `json:"spec"`
	Status struct {
		Network struct {
			ExternalIPv6   string `json:"externalIPv6"`
			SiloSubnetIPv6 string `json:"siloSubnetIPv6"`
		} `json:"network"`
	} `json:"status"`
}

// testWorkersIPv6: each worker's externalIPv6 is its Linode's SLAAC address (detected by the
// worker; nobody set it), and it has a silo IPv6 /64. It returns them by host name.
func testWorkersIPv6(t *testing.T, b *bed, sh func(string, string, ...string) (string, error), kvo string, workers map[string]machine,
	eventually func(string, time.Duration, func() (bool, string))) map[string]string {
	t.Helper()
	ctx := context.Background()
	ext := map[string]string{}
	for name, m := range workers {
		ips, err := b.API.GetInstanceIPAddresses(ctx, m.ID)
		if err != nil || ips.IPv6 == nil || ips.IPv6.SLAAC == nil || ips.IPv6.SLAAC.Address == "" {
			t.Fatalf("%s's SLAAC address from the Linode API: %+v %v", name, ips, err)
		}
		sa, err := netip.ParseAddr(ips.IPv6.SLAAC.Address)
		if err != nil {
			t.Fatalf("%s's SLAAC address %q: %v", name, ips.IPv6.SLAAC.Address, err)
		}
		slaac := sa.String() // canonical: the API may write it with leading zeros (fe7c:00d5)
		eventually(name+"'s externalIPv6 detected as its SLAAC address "+slaac, 3*time.Minute, func() (bool, string) {
			out, err := sh("", kvo, "get", "host", name, "-o", "json")
			var h hostIPv6
			if err != nil || json.Unmarshal([]byte(out), &h) != nil {
				return false, firstLine(out)
			}
			return h.Spec.ExternalIPv6 == "" && h.Status.Network.ExternalIPv6 == slaac && h.Status.Network.SiloSubnetIPv6 != "",
				fmt.Sprintf("spec %q, detected %q, silo /64 %q", h.Spec.ExternalIPv6, h.Status.Network.ExternalIPv6, h.Status.Network.SiloSubnetIPv6)
		})
		ext[name] = slaac
		t.Logf("%s: externalIPv6 %s (its SLAAC address, found by the worker)", name, slaac)
	}
	return ext
}

// testHostPortIPv6: a HostPort service on worker answers from outside over IPv6 on the worker's
// own IPv6, and the silo sees the client's own address (DNAT, not NAT); over IPv4 too. Then it
// goes.
func testHostPortIPv6(t *testing.T, sh func(string, string, ...string) (string, error), must func(string, string, ...string) string, kvo string,
	ext6 map[string]string, workers map[string]machine, eventually func(string, time.Duration, func() (bool, string))) {
	t.Helper()
	const port = 18150
	must(`apiVersion: kevaso/v1
kind: Silo
metadata: { name: six, namespace: e2e }
spec:
  runtime: docker
  container: { image: docker.io/library/python:3.12-slim, command: `+echo6Command("six", port)+` }
  resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web takes
  ports: [{ name: web, port: `+fmt.Sprint(port)+` }]
  services:
    - { name: six, type: HostPort, ports: [{ port: web }] }
`, kvo, "apply", "-f", "-")
	var s struct {
		Status struct {
			Phase    string `json:"phase"`
			Host     string `json:"host"`
			SiloIPv6 string `json:"siloIPv6"`
		} `json:"status"`
	}
	eventually("six running with a silo IPv6 address", 8*time.Minute, func() (bool, string) {
		out, err := sh("", kvo, "get", "silo", "six", "-n", "e2e", "-o", "json")
		if err == nil {
			_ = json.Unmarshal([]byte(out), &s)
		}
		return s.Status.Phase == "Running" && s.Status.SiloIPv6 != "", fmt.Sprintf("%+v", s.Status)
	})
	addr6 := ext6[s.Status.Host]
	t.Logf("six on %s: silo IPv6 %s, the worker's IPv6 %s", s.Status.Host, s.Status.SiloIPv6, addr6)
	if hereHasIPv6() {
		eventually(fmt.Sprintf("six answers over IPv6 on [%s]:%d, seeing this machine's own address", addr6, port), 3*time.Minute, func() (bool, string) {
			body, from, err := fromHere6(addr6, port)
			if err != nil {
				return false, err.Error()
			}
			return from != "" && body == fmt.Sprintf("six %d %s", port, from), body + " (sent from " + from + ")"
		})
		body, from, _ := fromHere6(addr6, port)
		t.Logf("http://[%s]:%d/ from outside over IPv6: %q (this machine sent from %s: the client's address kept)", addr6, port, body, from)
	} else {
		t.Errorf("this machine has no IPv6 route to the internet: the HostPort over IPv6 from outside is NOT tested")
	}
	pub := workers[s.Status.Host].Public
	eventually("and over IPv4 on the worker's address", 2*time.Minute, func() (bool, string) {
		body, err := get(fmt.Sprintf("http://%s:%d/", pub, port))
		return strings.HasPrefix(body, fmt.Sprintf("six %d ", port)), fmt.Sprint(body, err)
	})
	must("", kvo, "delete", "silo", "six", "-n", "e2e")
	eventually("six gone", 3*time.Minute, func() (bool, string) {
		_, err := sh("", kvo, "get", "silo", "six", "-n", "e2e")
		return err != nil, ""
	})
}

// testNodeBalancerIPv6: the NodeBalancer's IPv6 (the API's ipv6) is on the silo's status beside
// its IPv4, and the page answers there from outside (to a backend on private IPv4).
func testNodeBalancerIPv6(t *testing.T, b *bed, web siloStatus, eventually func(string, time.Duration, func() (bool, string))) {
	t.Helper()
	v4, v6 := web.Status.Service.LoadBalancerIP, web.Status.Service.LoadBalancerIPv6
	nbs, err := b.API.ListNodeBalancers(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	api6 := ""
	for _, nb := range nbs {
		if nb.IPv4 != nil && *nb.IPv4 == v4 && nb.IPv6 != nil {
			api6 = *nb.IPv6
		}
	}
	a, err := netip.ParseAddr(v6)
	if err != nil || !a.Is6() || api6 == "" || netip.MustParseAddr(api6) != a {
		t.Fatalf("status loadBalancerIPv6 %q: want the NodeBalancer's IPv6 from the API (%q) beside %s", v6, api6, v4)
	}
	t.Logf("web's NodeBalancer: %s and %s on its status (the API's ipv4 and ipv6)", v4, v6)
	if !hereHasIPv6() {
		t.Errorf("this machine has no IPv6 route to the internet: the NodeBalancer over IPv6 is NOT tested")
		return
	}
	eventually("the page through the NodeBalancer over IPv6", 3*time.Minute, func() (bool, string) {
		body, err := get("http://[" + v6 + "]/")
		return strings.Contains(body, "served from a Linode volume"), fmt.Sprint(body, err)
	})
	t.Logf("http://[%s]/ answers from the volume through the NodeBalancer over IPv6", v6)
}

// testNoFloatingIPv6: an IPv6 Provider IP (and a Dynamic IPv6 set) is refused at apply, saying
// why and what works instead; nothing is reserved.
func testNoFloatingIPv6(t *testing.T, sh func(string, string, ...string) (string, error), kvo, region string) {
	t.Helper()
	const want = "linode can't float an IPv6 address: a /64 moves as one unit; use an IPv4 Provider IP"
	for _, y := range []string{
		"apiVersion: kevaso/v1\nkind: IP\nmetadata: { name: edge6, namespace: e2e }\nspec: { type: Provider, provider: linode, region: " + region + ", family: IPv6 }\n",
		"apiVersion: kevaso/v1\nkind: IPSet\nmetadata: { name: pool6, namespace: e2e }\nspec: { type: Dynamic, provider: linode, region: " + region + ", max: 2, family: IPv6 }\n",
	} {
		out, err := sh(y, kvo, "apply", "-f", "-")
		if err == nil || !strings.Contains(out, want) {
			t.Fatalf("applied %s: %v %q; want a refusal saying %q", strings.Split(y, "\n")[1], err, out, want)
		}
		t.Logf("refused at apply: %s", strings.TrimSpace(out))
	}
	for _, kind := range [][2]string{{"ip", "edge6"}, {"ipset", "pool6"}} {
		if _, err := sh("", kvo, "get", kind[0], kind[1], "-n", "e2e"); err == nil {
			t.Fatalf("%s %s exists after the refusal", kind[0], kind[1])
		}
	}
}
