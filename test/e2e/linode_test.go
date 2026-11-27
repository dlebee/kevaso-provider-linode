//go:build e2e

// Package e2e runs Kevaso with this provider on real Linodes (M11's "done when"): a control plane
// and two workers, the plugin installed with kvoadm, and then a container silo with a Block Storage
// volume behind a NodeBalancer, its Ingress host name in Linode DNS, and a shared IP that follows
// its silo to the other worker when the first one goes down.
//
// It costs money and needs a token, so it only runs when asked:
//
//	KEVASO_LINODE_TOKEN_FILE=~/linode-token.txt go test -tags e2e -v -timeout 75m ./test/e2e/
//
// Everything it makes carries a tag of its own (kevaso-e2e-RUN) and is deleted at the end, also when
// it fails; then it checks the account holds nothing with that tag. KEVASO_E2E_KEEP=1 keeps a failed
// run's machines for a look (delete them with the sweep it prints).
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/linode/linodego"
)

type siloStatus struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Phase   string `json:"phase"`
		Host    string `json:"host"`
		Service struct {
			LoadBalancerIP   string `json:"loadBalancerIP"`
			LoadBalancerIPv6 string `json:"loadBalancerIPv6"`
			NodePorts        []struct {
				NodePort int `json:"nodePort"`
			} `json:"nodePorts"`
		} `json:"service"`
		Volumes []struct {
			Handle  string `json:"handle"`
			Device  string `json:"device"`
			Mounted bool   `json:"mounted"`
		} `json:"volumes"`
		Conditions []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func TestLinode(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kevaso-e2e-")
	t.Logf("run tag %s, region %s", b.Tag, b.Region)
	// Reserved IPv4s carry no tag, so the sweep can't find them: each one the run learns of is
	// released at the end (after the sweep has deleted the Linodes it was shared with), also when
	// the run fails before its IP is deleted. Released already (the run passed): a 404, fine.
	var reserved []string
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, a := range reserved {
			err := b.API.DeleteReservedIPAddress(dctx, a)
			var le *linodego.Error
			switch {
			case err == nil:
				t.Logf("released reserved %s (the run didn't)", a)
			case errors.As(err, &le) && le.Code == 404: // released by the run
			default:
				t.Errorf("release reserved %s: %v", a, err)
			}
		}
	})
	b.CleanupAtEnd(t)

	// Binaries: the CLIs for here, kevasod and the plugin for the Linodes.
	k := newKit(t, ctx)
	kvoadm, kvo := k.Kvoadm, k.Kvo
	kevasod := k.Build("kevasod", "kevaso.com/core/cmd/kevasod", "linux", "")
	plugin := k.Build("kevaso-provider-linode", "kevaso.com/provider-linode/cmd/kevaso-provider-linode", "linux", "")
	cl := k.Cluster()
	sh, must := cl.Run, cl.Must
	ssh := b.SSHFlags()
	eventually := func(what string, d time.Duration, f func() (bool, string)) {
		t.Helper()
		eventually(t, what, d, f)
	}
	silo := func(name string) siloStatus {
		var s siloStatus
		out, err := sh("", kvo, "get", "silo", name, "-n", "e2e", "-o", "json")
		if err == nil {
			_ = json.Unmarshal([]byte(out), &s)
		}
		return s
	}

	// 1. Three Linodes: a control plane and two workers. The Provider IP of step 7 is a reserved
	// IPv4 in the region (Linode's reserved IPs), not another Linode's.
	ms := b.CreateAll(ctx, "cp", "w1", "w2")
	cp, w1, w2 := ms["cp"], ms["w1"], ms["w2"]
	t.Cleanup(func() { // runs before the sweep: what was going on, if it failed
		if !t.Failed() {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for name, m := range map[string]machine{"cp": cp, "w1": w1, "w2": w2} {
			out, _ := b.SSH(dctx, m, "ls -la /run/kevaso-plugins/ /run/kevaso-plugins/* 2>&1; systemctl --no-pager status kevaso-plugin-linode 2>&1 | head -5; "+
				"journalctl -u kevaso-plugin-linode -n 30 --no-pager 2>&1; journalctl -u kevasod -n 60 --no-pager 2>&1 | grep -iv 'heartbeat' | tail -40; "+
				"ip -4 -br addr; journalctl -u 'kevaso-plugin*' -n 20 --no-pager 2>&1; nft list ruleset 2>&1 | grep -n 'dport\\|daddr\\|saddr' | head -40")
			t.Logf("── %s ──\n%s", name, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "yaml"}, {"get", "silos", "-A", "-o", "yaml"}, {"get", "ips", "-A", "-o", "yaml"}} {
			c := exec.CommandContext(dctx, kvo, args...)
			c.Env = cl.Env // the admin's, once kvoadm init ran
			out, _ := c.CombinedOutput()
			t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})

	// 2. The cluster: kvoadm init, two workers with docker.
	out := must("", kvoadm, append([]string{"init", "root@" + cp.Public, "--external-ip", cp.Public, "--kevasod", kevasod, "--name", "cp", "--cluster", "e2e",
		"--api-server", "https://" + cp.Public + ":6443"}, ssh...)...)
	if !regexp.MustCompile(`KVR1-`).MatchString(out) {
		t.Fatalf("no recovery key printed:\n%s", out)
	}
	for _, w := range []struct {
		name string
		m    machine
	}{{"w1", w1}, {"w2", w2}} {
		t.Log(must("", kvoadm, append([]string{"host", "add", "root@" + w.m.Public, "--external-ip", w.m.Public, "--kevasod", kevasod, "--name", w.name,
			"--install", "worker", "--runtime", "docker", "--region", b.Region}, ssh...)...))
	}

	// 3. The plugin: controller side on the control plane (with the token), node side on the workers.
	cfgFile := filepath.Join(t.TempDir(), "linode.json")
	_ = os.WriteFile(cfgFile, []byte(fmt.Sprintf(`{"tags": [%q], "labelPrefix": %q}`, b.Tag, b.Tag+"-")), 0o644)
	must("", kvo, "create", "clustersecret", "linode", "--from-file", "token="+tokenFile)
	must(grantClusterSecrets("linode", "linode"), kvo, "apply", "-f", "-")
	t.Log(must("", kvoadm, append([]string{"plugin", "install", "root@" + cp.Public, "--name", "linode", "--binary", plugin, "--manifest", manifestFile(t),
		"--cluster-secret", "linode", "--config", cfgFile}, ssh...)...))
	nodeCfg := filepath.Join(t.TempDir(), "node.json")
	_ = os.WriteFile(nodeCfg, []byte(fmt.Sprintf(`{"region": %q}`, b.Region)), 0o644)
	for _, w := range []machine{w1, w2} {
		t.Log(must("", kvoadm, append([]string{"plugin", "install", "root@" + w.Public, "--name", "linode", "--binary", plugin, "--manifest", manifestFile(t),
			"--side", "node", "--config", nodeCfg}, ssh...)...))
	}
	// kevasod fetches the token after install and caches it root-only on the control plane.
	eventually("the token cached on the control plane", 2*time.Minute, func() (bool, string) {
		out, err := b.SSH(ctx, cp, "stat -c '%a %U' /etc/kevaso/plugins/linode/linode/token")
		return strings.TrimSpace(out) == "600 root", fmt.Sprintf("%q %v", strings.TrimSpace(out), err)
	})
	if out, _ := b.SSH(ctx, w1, "ls /etc/kevaso/plugins/linode/"); strings.Contains(out, "linode") {
		t.Fatal("a worker got the token")
	}
	eventually("the workers recognised as Linodes", 3*time.Minute, func() (bool, string) {
		n := 0
		var seen []string
		for _, w := range []string{"w1", "w2"} {
			out, err := sh("", kvo, "get", "host", w, "-o", "json")
			var h struct {
				Status struct {
					Provider *struct{ Name, ID, Region, PrivateAddress string } `json:"provider"`
				} `json:"status"`
			}
			if err == nil && json.Unmarshal([]byte(out), &h) == nil && h.Status.Provider != nil && h.Status.Provider.Name == "linode" {
				n++
				seen = append(seen, fmt.Sprintf("%s=%+v", w, *h.Status.Provider))
			} else {
				seen = append(seen, w+": "+firstLine(out))
			}
		}
		return n == 2, strings.Join(seen, "; ")
	})
	// The IPv6 effort: each worker's own IPv6 is found (its SLAAC address).
	workers := map[string]machine{"w1": w1, "w2": w2}
	ext6 := testWorkersIPv6(t, b, sh, kvo, workers, eventually)

	// KEVASO_E2E_ONLY=provider-ip runs the Provider IP part alone (step 6: its holder down, a
	// confirmed move, the former holder back), skipping 4, 5 and 7–8.
	focus := os.Getenv("KEVASO_E2E_ONLY") == "provider-ip"
	nb := false // the token can use NodeBalancers (step 4 finds out)
	if focus {
		must("apiVersion: kevaso/v1\nkind: Namespace\nmetadata: { name: e2e }\n", kvo, "apply", "-f", "-")
		t.Log("KEVASO_E2E_ONLY=provider-ip: only the Provider IP part (step 6)")
	} else {
		// 4. A container silo with a Block Storage volume behind a NodeBalancer, and an Ingress class
		// publishing its host name in Linode DNS.
		zone := b.Tag + ".kevaso.com"
		dns := true
		if _, err := b.API.CreateDomain(ctx, linodego.DomainCreateOptions{Domain: zone, Type: linodego.DomainTypeMaster,
			SOAEmail: "hostmaster@kevaso.com", Tags: []string{b.Tag}}); err != nil {
			var le *linodego.Error
			if !errors.As(err, &le) || (le.Code != 401 && le.Code != 403) {
				t.Fatalf("test zone: %v", err)
			}
			dns = false
			t.Logf("SKIPPING the DNS part: this token can't use Domains (%v); give it Domains read/write to test it", err)
		}
		nb = true
		if _, err := b.API.ListNodeBalancers(ctx, nil); err != nil {
			var le *linodego.Error
			if !errors.As(err, &le) || (le.Code != 401 && le.Code != 403) {
				t.Fatalf("NodeBalancers: %v", err)
			}
			nb, dns = false, false // the Ingress's address would be the NodeBalancer's
			t.Logf("SKIPPING the NodeBalancer part: this token can't use NodeBalancers (%v); give it NodeBalancers read/write to test it", err)
		}
		// One NodeBalancer for the service (networking v2: one per LoadBalancer service by default).
		exposure := `
  ports: [{ name: http, port: 80 }]
  services:
    - name: web
      type: LoadBalancer
      loadBalancer: { provider: linode }
      ports: [{ port: http }]
`
		if !nb {
			exposure = `
  ports: [{ name: http, port: 80 }]
  services:
    - { name: web, ports: [{ port: http }] }
`
		}
		// web is also the class's controller: the nginx driver writes its nginx.conf (one server per
		// Ingress host, 404 for any other), so the Ingress goes in after the page has been fetched at
		// the bare address.
		ingress := `apiVersion: kevaso/v1
kind: IngressClass
metadata: { name: linode }
spec:
  controller: { driver: nginx, silo: web, namespace: e2e }
  dns: { provider: linode, ttl: 30 }
  hosts: [{ host: shop.` + zone + `, namespaces: [e2e] }]   # a host must be assigned to be claimed (core #12)
---
apiVersion: kevaso/v1
kind: Ingress
metadata: { name: shop, namespace: e2e }
spec:
  ingressClass: linode
  hosts: [shop.` + zone + `]
  tls: { none: true }
  rules: [{ path: /, service: { name: web, port: http } }]
`
		must(`apiVersion: kevaso/v1
kind: Namespace
metadata: { name: e2e }
---
apiVersion: kevaso/v1
kind: Silo
metadata: { name: web, namespace: e2e }
spec:
  runtime: docker
  container: { image: docker.io/library/nginx:1.27 }
  resources: { cpu: 1, memory: 256Mi }
  volumes:
    - { name: site, class: linode-block, size: 10Gi, mountPath: /usr/share/nginx/html }
  init:
    steps:
      - name: page
        run: |
          [ -f /usr/share/nginx/html/index.html ] || echo "served from a Linode volume" > /usr/share/nginx/html/index.html`+exposure, kvo, "apply", "-f", "-")
		var web siloStatus
		eventually("web running with its volume and NodeBalancer", 10*time.Minute, func() (bool, string) {
			web = silo("web")
			ok := web.Status.Phase == "Running" && (web.Status.Service.LoadBalancerIP != "" || !nb) && len(web.Status.Volumes) == 1 && web.Status.Volumes[0].Mounted
			return ok, fmt.Sprintf("%+v", web.Status)
		})
		t.Logf("web on %s, NodeBalancer %s, volume %s at %s", web.Status.Host, web.Status.Service.LoadBalancerIP, web.Status.Volumes[0].Handle, web.Status.Volumes[0].Device)
		host := map[string]machine{"w1": w1, "w2": w2}[web.Status.Host]
		if out, _ := b.SSH(ctx, host, "findmnt -n -o SOURCE,FSTYPE /var/lib/kevaso/volumes/"+web.Metadata.UID+"/site"); !strings.Contains(out, "ext4") {
			t.Fatalf("volume mount on %s: %q", web.Status.Host, out)
		}
		if got := must("", kvo, "exec", "silo", "web", "-n", "e2e", "--", "cat", "/usr/share/nginx/html/index.html"); !strings.Contains(got, "served from a Linode volume") {
			t.Fatalf("the page on the volume: %q", got)
		}
		t.Log("the init step wrote the page onto the Linode volume")
		if nb {
			testNodeBalancer(t, web, host, eventually)
			testNodeBalancerIPv6(t, b, web, eventually)
		}
		// 4a. IPv6 in on a worker's own address: a HostPort service, the client's address kept.
		testHostPortIPv6(t, sh, must, kvo, ext6, workers, eventually)
		if dns {
			must(ingress, kvo, "apply", "-f", "-")
			eventually("shop."+zone+" in Linode DNS", 20*time.Minute, func() (bool, string) {
				addrs, err := lookup(ctx, "shop."+zone)
				return err == nil && len(addrs) == 1 && addrs[0] == web.Status.Service.LoadBalancerIP, fmt.Sprint(addrs, err)
			})
			t.Logf("shop.%s → %s on ns1.linode.com", zone, web.Status.Service.LoadBalancerIP)
			must("", kvo, "delete", "ingress", "shop", "-n", "e2e")
			// The class is cluster-wide and names e2e (its controller, its hosts): a namespace named from
			// outside isn't deleted (step 8), so it goes with the DNS part.
			must("", kvo, "delete", "ingressclass", "linode")
		}

		// 4b. Two silos sharing one NodeBalancer by name (linode.kevaso.com/nodebalancer: edge), each on
		// its own port: one address, each port to its own silo; it stays while one is left, and goes
		// with the last.
		if nb {
			testSharedNodeBalancer(t, b, cl.Run, cl.Must, kvo, eventually)
		}

		// 5. Delete it all: the record, the NodeBalancer and the volume go with them.
		must("", kvo, "delete", "silo", "web", "-n", "e2e")
		eventually("web, its NodeBalancer, volume and record gone", 10*time.Minute, func() (bool, string) {
			_, err := sh("", kvo, "get", "silo", "web", "-n", "e2e")
			left := b.Remaining(ctx)
			var cloud []string
			for _, l := range left {
				if !strings.HasPrefix(l, "Linode ") && !strings.HasPrefix(l, "domain ") {
					cloud = append(cloud, l)
				}
			}
			recs := 0
			if ds, _ := b.API.ListDomains(ctx, nil); len(ds) > 0 {
				for _, d := range ds {
					if d.Domain == zone {
						rs, _ := b.API.ListDomainRecords(ctx, d.ID, nil)
						for _, r := range rs {
							if r.Type == linodego.RecordTypeA || r.Type == linodego.RecordTypeTXT {
								recs++
							}
						}
					}
				}
			}
			return err != nil && len(cloud) == 0 && recs == 0, fmt.Sprintf("silo err=%v cloud=%v records=%d", err, cloud, recs)
		})
		switch {
		case nb && dns:
			t.Log("deleting the silo deleted its NodeBalancer, its volume and its DNS records")
		case nb:
			t.Log("deleting the silo deleted its NodeBalancer and its volume")
		default:
			t.Log("deleting the silo detached and deleted its volume")
		}

		// 5b. No floating IPv6 on Linode: an IPv6 Provider IP is refused at apply, with the reason.
		testNoFloatingIPv6(t, sh, kvo, b.Region)

	}

	// 6. A Provider IP that follows its silo: an IP of type Provider, a reserved IPv4 in the
	// region, shared with the region's Linodes and announced over BGP by the silo's host (a
	// HostPort service on it). That host is shut down. edge opted in to stop without the control
	// plane, so it is restarted elsewhere only after its own host reports FenceStopped for its latest
	// start; a host that is shut down can't report, so nothing starts it on the other worker. A person
	// runs kvo move silo edge -n e2e --to HOST --confirm-fenced --recreate, and the address answers there.
	// Its traffic out is checked against api.ipify.org, by address: nothing leaves without a Grant,
	// and a grant to a name goes through the egress gate (not from the silo's own source), so the
	// grant opens the name's IPv4 addresses (resolved here) on port 80 and edge asks one by address.
	ipify := ipv4sOf(t, "api.ipify.org")
	var ipifyRules strings.Builder
	for _, a := range ipify {
		fmt.Fprintf(&ipifyRules, "    - { to: %q, ports: [\"80\"] }\n", a+"/32")
	}
	must(`apiVersion: kevaso/v1
kind: Grant
metadata: { name: ipify, namespace: e2e }
spec:
  egress:
`+ipifyRules.String()+`---
apiVersion: kevaso/v1
kind: IP
metadata: { name: edge, namespace: e2e }
spec: { type: Provider, provider: linode, region: `+b.Region+` }
---
apiVersion: kevaso/v1
kind: Silo
metadata: { name: edge, namespace: e2e }
spec:
  runtime: docker
  container:
    image: docker.io/library/nginx:1.27-alpine
    command: [sh, -c, "echo edge answers > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
  resources: { cpu: 1, memory: 256Mi }
  availability: { onHostLoss: Recreate, onControlPlaneLoss: Stop, after: 30s }
  grants: [ipify]
  ports: [{ name: http, port: 80 }]
  services:
    - name: edge
      type: HostPort
      hostPort: { ip: edge }
      ports: [{ port: http, as: 80 }]
  network:
    egress: { source: { ip: edge } }   # out from the address it answers on
`, kvo, "apply", "-f", "-")
	reservedAt := time.Now()
	var addr string
	eventually("the Provider IP reserved", 3*time.Minute, func() (bool, string) {
		out, err := sh("", kvo, "get", "ip", "edge", "-n", "e2e", "-o", "json")
		var ip struct {
			Status struct {
				Address string `json:"address"`
			} `json:"status"`
		}
		if err == nil {
			_ = json.Unmarshal([]byte(out), &ip)
		}
		addr = ip.Status.Address
		return addr != "", firstLine(out)
	})
	reserved = append(reserved, addr)
	if r, err := b.API.GetReservedIPAddress(ctx, addr); err != nil || r.Region != b.Region {
		t.Fatalf("%s isn't a reserved IPv4 in %s: %+v %v", addr, b.Region, r, err)
	}
	t.Logf("IP edge reserved as %s (a reserved IPv4 in %s) in %s", addr, b.Region, time.Since(reservedAt).Round(time.Second))
	var edge siloStatus
	eventually("edge running on the Provider IP", 8*time.Minute, func() (bool, string) {
		edge = silo("edge")
		return edge.Status.Phase == "Running", fmt.Sprintf("%+v", edge.Status)
	})
	blocked := sharingRefused(t, sh, kvo, "edge", eventually)
	if blocked == "" {
		first := edge.Status.Host
		eventually("http://"+addr+"/ answered by edge on "+first, 5*time.Minute, func() (bool, string) {
			body, err := get("http://" + addr + "/")
			return strings.Contains(body, "edge answers"), fmt.Sprint(body, err)
		})
		t.Logf("%s answers from edge on %s (announced over BGP), %s after the apply", addr, first, time.Since(reservedAt).Round(time.Second))
		// Out from the same address: what an outside server sees edge's requests come from.
		egressFrom := func(want string) {
			t.Helper()
			eventually("edge's traffic out from "+want, 3*time.Minute, func() (bool, string) {
				out, err := sh("", kvo, "exec", "silo", "edge", "-n", "e2e", "--", "wget", "-qO-", "-T", "10", "--header", "Host: api.ipify.org", "http://"+ipify[0]+"/")
				return err == nil && strings.TrimSpace(out) == want, fmt.Sprint(strings.TrimSpace(out), err)
			})
		}
		egressFrom(addr)
		t.Logf("edge's traffic out leaves from %s (egress.source ip: edge)", addr)
		down := map[string]machine{"w1": w1, "w2": w2}[first]
		if err := b.API.ShutdownInstance(ctx, down.ID); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s shut down", first)
		start := time.Now()
		// About a minute: the shut-down host cannot report FenceStopped, so edge is not started on the
		// other worker by itself. A condition names kvo move --confirm-fenced when this test can read it.
		otherHost := other(first)
		var named string
		for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
			edge = silo("edge")
			if edge.Status.Host != "" && edge.Status.Host != first {
				t.Fatalf("edge started on %s by itself after %s shut down: %+v", edge.Status.Host, first, edge.Status)
			}
			for _, c := range edge.Status.Conditions {
				if strings.Contains(c.Message, "--confirm-fenced") {
					named = c.Message
				}
			}
		}
		if named == "" {
			t.Fatalf("edge's conditions never named --confirm-fenced while %s was shut down: %+v", first, edge.Status.Conditions)
		}
		t.Logf("edge was not started on %s by itself; a condition names the move: %s", otherHost, named)
		moved := must("", kvo, "move", "silo", "edge", "-n", "e2e", "--to", otherHost, "--confirm-fenced", "--recreate")
		t.Logf("kvo move edge --confirm-fenced --recreate: %s", strings.TrimSpace(moved))
		eventually("edge running on the other worker", 6*time.Minute, func() (bool, string) {
			edge = silo("edge")
			return edge.Status.Host != "" && edge.Status.Host != first && edge.Status.Phase == "Running", fmt.Sprintf("%+v", edge.Status)
		})
		eventually("the Provider IP answering from "+edge.Status.Host, 3*time.Minute, func() (bool, string) {
			body, err := get("http://" + addr + "/")
			return strings.Contains(body, "edge answers"), fmt.Sprint(body, err)
		})
		t.Logf("%s answers again, from edge on %s, %s after %s went down", addr, edge.Status.Host, time.Since(start).Round(time.Second), first)
		egressFrom(addr)
		t.Logf("after the move edge's traffic still leaves from %s", addr)
		// The shut-down worker comes back: the address stays with edge where it is.
		b.PowerOn(ctx, down)
		eventually(first+" Ready again", 5*time.Minute, func() (bool, string) {
			hs, err := cl.Hosts()
			return err == nil && hs[first].Ready(), fmt.Sprint(err)
		})
		if body, err := get("http://" + addr + "/"); !strings.Contains(body, "edge answers") {
			t.Fatalf("%s stopped answering when %s came back: %q %v", addr, first, body, err)
		}
		// Linode's Network Helper puts the reserved IPv4 assigned to the former holder back on its
		// eth0 at boot; kevasod takes it off (a host has a Provider IP only where it holds it).
		eventually(addr+" on none of "+first+"'s interfaces", 3*time.Minute, func() (bool, string) {
			out, err := b.SSH(ctx, down, "ip -o addr show | grep -F ' "+addr+"/' ; true")
			return err == nil && strings.TrimSpace(out) == "", strings.TrimSpace(out)
		})
		logs, _ := b.SSH(ctx, down, "journalctl -u kevasod --no-pager -b | grep -F 'removed Provider IP "+addr+"' | head -3")
		t.Logf("%s holds no copy of %s on any interface after its boot (kevasod: %s)", first, addr, strings.TrimSpace(logs))
		t.Logf("%s is back; %s still answers from edge on %s", first, addr, edge.Status.Host)
	}

	// The silo goes, the IP stays (the next silo naming it gets the same address), shared nowhere;
	// deleting the IP releases the reservation.
	must("", kvo, "delete", "silo", "edge", "-n", "e2e")
	eventually("edge's shares removed", 5*time.Minute, func() (bool, string) {
		ips, err := b.API.GetInstanceIPAddresses(ctx, map[string]machine{"w1": w1, "w2": w2}[edge.Status.Host].ID)
		if err != nil || ips.IPv4 == nil {
			return false, fmt.Sprint(err)
		}
		return len(ips.IPv4.Shared) == 0, fmt.Sprint(len(ips.IPv4.Shared))
	})
	if _, err := b.API.GetReservedIPAddress(ctx, addr); err != nil {
		t.Fatalf("the IP's reservation went with the silo: %v", err)
	}
	must("", kvo, "delete", "ip", "edge", "-n", "e2e")
	eventually("the reservation released with the IP", 5*time.Minute, func() (bool, string) {
		_, err := b.API.GetReservedIPAddress(ctx, addr)
		var le *linodego.Error
		return errors.As(err, &le) && le.Code == 404, fmt.Sprint(err)
	})
	t.Logf("%s released with its IP", addr)

	// 7. An IPSet of type Dynamic: each SiloSet instance its own Provider IP by ordinal, and
	// 8. the namespace deleted with everything in it (kvo delete namespace, after its question).
	if !focus {
		testIPSetAndCascade(t, b, cl, kvo, []machine{cp, w1, w2}, nb, eventually, func(a string) { reserved = append(reserved, a) })
	}
}

// sharingRefused waits for IP name's Shared condition: "" once it is shared with its holder's
// Linode; Linode's refusal when the account can't share it (a reserved IPv4 must be assigned to a
// Linode before it can be shared, and a Linode with its one public IPv4 can't take another without
// a limit raised by Linode support). Then the test records the failure and goes on without the
// parts that need the address to answer.
func sharingRefused(t *testing.T, sh func(string, string, ...string) (string, error), kvo, name string, eventually func(string, time.Duration, func() (bool, string))) string {
	t.Helper()
	var why string
	eventually("IP "+name+" shared with its holder's Linode (or refused)", 3*time.Minute, func() (bool, string) {
		out, err := sh("", kvo, "get", "ip", name, "-n", "e2e", "-o", "json")
		var ip struct {
			Status struct {
				Conditions []struct{ Type, Status, Reason, Message string } `json:"conditions"`
			} `json:"status"`
		}
		if err != nil || json.Unmarshal([]byte(out), &ip) != nil {
			return false, firstLine(out)
		}
		for _, c := range ip.Status.Conditions {
			if c.Type == "Shared" && c.Status == "True" {
				return true, ""
			}
			// Waiting, naming the fix: Linode's per-Linode IPv4 limit, which its support raises.
			if c.Type == "Shared" && c.Status == "False" && c.Reason == "Waiting" && strings.Contains(c.Message, "Ask Linode support to allow more IPv4 addresses per Linode") {
				why = c.Message
				return true, ""
			}
		}
		return false, fmt.Sprintf("%+v", ip.Status.Conditions)
	})
	if why != "" {
		t.Errorf("BLOCKED: IP %s can't be shared on this account: %s. Not tested: its address answering, egress from it, following a move", name, why)
	}
	return why
}

// testIPSetAndCascade: a Dynamic IPSet gives each instance of a SiloSet its own Provider IP
// (pool-0, pool-1, reserved as the instances need them), each answering from its own instance;
// then kvo delete namespace e2e, answered yes at its question, removes the silos, the balancer
// and the IPs in order and leaves nothing in the account.
func testIPSetAndCascade(t *testing.T, b *bed, cl *cluster, kvo string, ms []machine, nb bool, eventually func(string, time.Duration, func() (bool, string)),
	reserved func(string)) {
	t.Helper()
	ctx := context.Background()
	sh, must := cl.Run, cl.Must
	lb := ""
	if nb {
		lb = `---
apiVersion: kevaso/v1
kind: Silo
metadata: { name: solo, namespace: e2e }
spec:
  runtime: docker
  container:
    image: docker.io/library/nginx:1.27-alpine
    command: [sh, -c, "echo solo answers > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
  resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web or edge takes
  ports: [{ name: http, port: 80 }]
  services:
    - { name: solo, type: LoadBalancer, loadBalancer: { provider: linode }, ports: [{ port: http }] }
`
	}
	applied := time.Now()
	must(`apiVersion: kevaso/v1
kind: IPSet
metadata: { name: pool, namespace: e2e }
spec: { type: Dynamic, provider: linode, region: `+b.Region+`, max: 2 }
---
apiVersion: kevaso/v1
kind: SiloSet
metadata: { name: pool, namespace: e2e }
spec:
  instances: 2
  ips: pool
  template:
    runtime: docker
    container:
      image: docker.io/library/nginx:1.27-alpine
      command: [sh, -c, "hostname > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
    resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web or edge takes
    ports: [{ name: http, port: 80 }]
    services:
      - type: HostPort
        hostPort: { own: true }   # each instance its own member of pool
        ports: [{ port: http, as: 80 }]
`+lb, kvo, "apply", "-f", "-")
	type ipView struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			Type string `json:"type"`
		} `json:"spec"`
		Status struct {
			Address string `json:"address"`
			Holder  *struct {
				Holder string `json:"holder"` // silo/NAME
			} `json:"holder"`
		} `json:"status"`
	}
	addrs := map[string]string{} // member IP → address
	eventually("pool-0 and pool-1 reserved, each held by its instance", 8*time.Minute, func() (bool, string) {
		var why []string
		for _, n := range []string{"pool-0", "pool-1"} {
			out, err := sh("", kvo, "get", "ip", n, "-n", "e2e", "-o", "json")
			var ip ipView
			if err != nil || json.Unmarshal([]byte(out), &ip) != nil {
				why = append(why, n+": "+firstLine(out))
				continue
			}
			holder := ""
			if ip.Status.Holder != nil {
				holder = ip.Status.Holder.Holder
			}
			if ip.Spec.Type != "Provider" || ip.Status.Address == "" || holder != "silo/"+n || ip.Metadata.Labels["kevaso.com/ipset"] != "pool" {
				why = append(why, fmt.Sprintf("%s: type %s address %q holder %q labels %v", n, ip.Spec.Type, ip.Status.Address, holder, ip.Metadata.Labels))
				continue
			}
			if addrs[n] == "" {
				reserved(ip.Status.Address)
			}
			addrs[n] = ip.Status.Address
		}
		return len(why) == 0 && addrs["pool-0"] != addrs["pool-1"], strings.Join(why, "; ")
	})
	for n, a := range addrs {
		if r, err := b.API.GetReservedIPAddress(ctx, a); err != nil || r.Region != b.Region {
			t.Fatalf("%s's %s isn't a reserved IPv4 in %s: %+v %v", n, a, b.Region, r, err)
		}
	}
	t.Logf("IPSet pool: pool-0 %s, pool-1 %s, reserved IPv4s in %s, %s after the apply", addrs["pool-0"], addrs["pool-1"], b.Region, time.Since(applied).Round(time.Second))
	for _, n := range []string{"pool-0", "pool-1"} {
		if sharingRefused(t, sh, kvo, n, eventually) != "" {
			continue
		}
		var name string
		eventually(n+"'s own address answered by "+n, 8*time.Minute, func() (bool, string) {
			hn, err := sh("", kvo, "exec", "silo", n, "-n", "e2e", "--", "hostname")
			if err != nil {
				return false, firstLine(hn)
			}
			name = strings.TrimSpace(hn)
			body, err := get("http://" + addrs[n] + "/")
			return strings.TrimSpace(body) == name, fmt.Sprintf("hostname %q, http://%s/ %q %v", name, addrs[n], strings.TrimSpace(body), err)
		})
		var s siloStatus
		if out, err := sh("", kvo, "get", "silo", n, "-n", "e2e", "-o", "json"); err == nil {
			_ = json.Unmarshal([]byte(out), &s)
		}
		t.Logf("http://%s/ answers from %s (hostname %s) on %s", addrs[n], n, name, s.Status.Host)
	}
	t.Logf("each instance has its own Provider IP (answering where Linode shares it), %s after the apply", time.Since(applied).Round(time.Second))
	var soloAddr string
	if nb {
		eventually("solo behind its own NodeBalancer", 10*time.Minute, func() (bool, string) {
			var s siloStatus
			if out, err := sh("", kvo, "get", "silo", "solo", "-n", "e2e", "-o", "json"); err == nil {
				_ = json.Unmarshal([]byte(out), &s)
			}
			soloAddr = s.Status.Service.LoadBalancerIP
			if soloAddr == "" {
				return false, fmt.Sprintf("%+v", s.Status)
			}
			body, err := get("http://" + soloAddr + "/")
			return strings.Contains(body, "solo answers"), fmt.Sprint(body, err)
		})
		t.Logf("solo answers through its NodeBalancer %s", soloAddr)
	}

	// 8. kvo delete namespace e2e: with nobody to ask it refuses and names --yes; at a terminal it
	// lists what is there, asks, and on yes deletes it all in order.
	out, err := sh("", kvo, "delete", "namespace", "e2e")
	if err == nil || !strings.Contains(out, "--yes") || !strings.Contains(out, "still holds") {
		t.Fatalf("kvo delete namespace with no terminal: want a refusal naming --yes, got %v:\n%s", err, out)
	}
	t.Logf("with no terminal: %s", firstLine(out))
	cascadeAt := time.Now()
	answer, err := kvoAtTerminal(cl, kvo, "Are you sure?", "yes\n", 15*time.Minute, "delete", "namespace", "e2e")
	if err != nil || !strings.Contains(answer, "namespace/e2e deleted") {
		t.Fatalf("kvo delete namespace e2e, answered yes: %v\n%s", err, answer)
	}
	t.Logf("kvo delete namespace e2e (answered yes), %s:\n%s", time.Since(cascadeAt).Round(time.Second), answer)
	if _, err := sh("", kvo, "get", "namespace", "e2e"); err == nil {
		t.Fatal("namespace e2e still there")
	}
	eventually("nothing of e2e left in the account", 5*time.Minute, func() (bool, string) {
		var left []string
		for _, l := range b.Remaining(ctx) {
			if !strings.HasPrefix(l, "Linode ") && !strings.HasPrefix(l, "domain ") {
				left = append(left, l)
			}
		}
		for n, a := range addrs {
			if _, err := b.API.GetReservedIPAddress(ctx, a); err == nil {
				left = append(left, n+" reserved "+a)
			}
		}
		for _, m := range ms {
			if ips, err := b.API.GetInstanceIPAddresses(ctx, m.ID); err != nil || ips.IPv4 == nil {
				left = append(left, fmt.Sprint(m.Label, " ", err))
			} else if len(ips.IPv4.Shared) > 0 {
				left = append(left, fmt.Sprintf("%s shares %d", m.Label, len(ips.IPv4.Shared)))
			}
		}
		return len(left) == 0, strings.Join(left, "; ")
	})
	t.Logf("namespace e2e deleted with its silos, NodeBalancer and IPs (released, shared nowhere): %s in all", time.Since(cascadeAt).Round(time.Second))
}

// kvoAtTerminal runs kvo on a terminal (a pty), waits for prompt and types answer: a person at
// kvo's question.
func kvoAtTerminal(cl *cluster, kvo, prompt, answer string, d time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	cmd := exec.CommandContext(ctx, kvo, args...)
	cmd.Env = cl.Env
	f, err := pty.Start(cmd)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	var mu sync.Mutex
	var buf strings.Builder
	done := make(chan struct{})
	go func() {
		defer close(done)
		p := make([]byte, 4096)
		for {
			n, err := f.Read(p)
			mu.Lock()
			buf.Write(p[:n])
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	seen := func() string { mu.Lock(); defer mu.Unlock(); return buf.String() }
	for !strings.Contains(seen(), prompt) {
		select {
		case <-ctx.Done():
			return seen(), fmt.Errorf("no %q: %w", prompt, ctx.Err())
		case <-done:
			return seen(), fmt.Errorf("kvo ended before asking: %w", cmd.Wait())
		case <-time.After(200 * time.Millisecond):
		}
	}
	if _, err := f.Write([]byte(answer)); err != nil {
		return seen(), err
	}
	err = cmd.Wait()
	<-done
	return seen(), err
}

// testSharedNodeBalancer: two silos, one NodeBalancer named edge, each on its own port.
func testSharedNodeBalancer(t *testing.T, b *bed, sh func(string, string, ...string) (string, error), must func(string, string, ...string) string, kvo string,
	eventually func(string, time.Duration, func() (bool, string))) {
	t.Helper()
	ctx := context.Background()
	start := time.Now()
	// A misspelled annotation is refused at apply (the plugin declares its annotations).
	typo := `apiVersion: kevaso/v1
kind: Silo
metadata: { name: shop-typo, namespace: e2e }
spec:
  runtime: docker
  container: { image: docker.io/library/nginx:1.27-alpine }
  resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web or edge takes
  ports: [{ name: http, port: 80 }]
  services:
    - type: LoadBalancer
      loadBalancer: { provider: linode }
      annotations: { linode.kevaso.com/nodebalancr: edge }
      ports: [{ port: http }]
`
	if out, err := sh(typo, kvo, "apply", "-f", "-"); err == nil || !strings.Contains(out, "nodebalancr") {
		t.Fatalf("a misspelled annotation: want a refusal naming it, got %v:\n%s", err, out)
	} else {
		t.Logf("linode.kevaso.com/nodebalancr refused at apply: %s", strings.TrimSpace(out))
	}
	if _, err := sh("", kvo, "get", "silo", "shop-typo", "-n", "e2e"); err == nil {
		t.Fatal("shop-typo was made although its apply was refused")
	}
	// shop-a: one service, routed as soon as it runs. shop-b: a readiness probe and two services
	// on the same port: shop-b-live (routeWhen: running, as 8081) and shop-b (routeWhen: ready,
	// as 8080), so 8080 has shop-b as its backend only while it is ready.
	must(`apiVersion: kevaso/v1
kind: Silo
metadata: { name: shop-a, namespace: e2e }
spec:
  runtime: docker
  container:
    image: docker.io/library/nginx:1.27-alpine
    command: [sh, -c, "echo shop-a answers > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
  resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web or edge takes
  ports: [{ name: http, port: 80 }]
  services:
    - name: shop-a
      type: LoadBalancer
      loadBalancer: { provider: linode }
      annotations: { linode.kevaso.com/nodebalancer: edge }
      ports: [{ port: http, as: 80 }]
---
apiVersion: kevaso/v1
kind: Silo
metadata: { name: shop-b, namespace: e2e }
spec:
  runtime: docker
  container:
    image: docker.io/library/nginx:1.27-alpine
    command: [sh, -c, "echo shop-b answers > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
  resources: { memory: 128Mi }   # no CPU asked: a nanode has 1, which web or edge takes
  ports: [{ name: http, port: 80 }]
  probes:
    readiness: { exec: { command: [test, -f, /tmp/ready] }, period: 5s, failures: 2 }
  services:
    - name: shop-b-live
      type: LoadBalancer
      loadBalancer: { provider: linode }
      annotations: { linode.kevaso.com/nodebalancer: edge }
      routeWhen: running
      ports: [{ port: http, as: 8081 }]
    - name: shop-b
      type: LoadBalancer
      loadBalancer: { provider: linode }
      annotations: { linode.kevaso.com/nodebalancer: edge }
      routeWhen: ready
      ports: [{ port: http, as: 8080 }]
`, kvo, "apply", "-f", "-")
	address := func(name string) string {
		var s siloStatus
		if out, err := sh("", kvo, "get", "silo", name, "-n", "e2e", "-o", "json"); err == nil {
			_ = json.Unmarshal([]byte(out), &s)
		}
		return s.Status.Service.LoadBalancerIP
	}
	ours := func() []linodego.NodeBalancer {
		all, _ := b.API.ListNodeBalancers(ctx, nil)
		var out []linodego.NodeBalancer
		for _, nb := range all {
			if slices.Contains(nb.Tags, b.Tag) {
				out = append(out, nb)
			}
		}
		return out
	}
	// configs: what Linode has on the shared NodeBalancer, port → backends.
	configs := func() string {
		var out []string
		for _, nb := range ours() {
			if nb.Label == nil || !strings.Contains(*nb.Label, "edge") {
				continue
			}
			cs, err := b.API.ListNodeBalancerConfigs(ctx, nb.ID, nil)
			if err != nil {
				return err.Error()
			}
			for _, c := range cs {
				nodes, _ := b.API.ListNodeBalancerNodes(ctx, nb.ID, c.ID, nil)
				var at []string
				for _, n := range nodes {
					at = append(at, n.Address)
				}
				out = append(out, fmt.Sprintf("%d→%s", c.Port, strings.Join(at, ",")))
			}
			out = append(out, "tags "+strings.Join(nb.Tags, ","))
		}
		return "NodeBalancer edge: " + strings.Join(out, " ")
	}
	answers := func(url, want string) (bool, string) {
		body, err := get(url)
		return strings.Contains(body, want), fmt.Sprintf("%q %v", strings.TrimSpace(body), err)
	}
	var addr string
	eventually("shop-a and shop-b on one NodeBalancer", 10*time.Minute, func() (bool, string) {
		a, bb := address("shop-a"), address("shop-b")
		addr = a
		return a != "" && a == bb && len(ours()) == 2, fmt.Sprintf("shop-a %q, shop-b %q, NodeBalancers %d (web's and edge)", a, bb, len(ours()))
	})
	t.Logf("shop-a and shop-b on one NodeBalancer %s, %s after the apply", addr, time.Since(start).Round(time.Second))
	for url, want := range map[string]string{"http://" + addr + "/": "shop-a answers", "http://" + addr + ":8081/": "shop-b answers"} {
		eventually(url+" → "+want, 5*time.Minute, func() (bool, string) { return answers(url, want) })
	}
	t.Logf("one NodeBalancer %s: port 80 → shop-a, port 8081 → shop-b (routeWhen: running), %s after the apply", addr, time.Since(start).Round(time.Second))
	// shop-b isn't ready: nothing answers on 8080 for a while.
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Second) {
		if ok, got := answers("http://"+addr+":8080/", "shop-b answers"); ok {
			t.Fatalf("port 8080 (routeWhen: ready) answered by shop-b before it was ready: %s", got)
		}
	}
	t.Log("port 8080 (routeWhen: ready) has no backend while shop-b isn't ready")
	must("", kvo, "exec", "silo", "shop-b", "-n", "e2e", "--", "touch", "/tmp/ready")
	ready := time.Now()
	eventually("8080 → shop-b once ready", 5*time.Minute, func() (bool, string) { return answers("http://"+addr+":8080/", "shop-b answers") })
	t.Logf("shop-b ready: 8080 answers from it %s later", time.Since(ready).Round(time.Second))
	must("", kvo, "exec", "silo", "shop-b", "-n", "e2e", "--", "rm", "/tmp/ready")
	unready := time.Now()
	eventually("8080 without shop-b once not ready, 8081 still on it", 5*time.Minute, func() (bool, string) {
		on8080, got := answers("http://"+addr+":8080/", "shop-b answers")
		on8081, got1 := answers("http://"+addr+":8081/", "shop-b answers")
		return !on8080 && on8081, "8080 " + got + ", 8081 " + got1 + "; " + configs()
	})
	t.Logf("shop-b not ready: off 8080 %s later, still on 8081 (routeWhen per service)", time.Since(unready).Round(time.Second))
	must("", kvo, "delete", "silo", "shop-a", "-n", "e2e")
	eventually("edge kept for shop-b, port 80 gone", 5*time.Minute, func() (bool, string) {
		ok, got := answers("http://"+addr+":8081/", "shop-b answers")
		_, err80 := get("http://" + addr + "/")
		return ok && err80 != nil && len(ours()) == 2, fmt.Sprint(got, err80, len(ours()))
	})
	gone := time.Now()
	must("", kvo, "delete", "silo", "shop-b", "-n", "e2e")
	eventually("edge removed with its last service", 5*time.Minute, func() (bool, string) {
		return len(ours()) == 1, fmt.Sprint(len(ours()), " NodeBalancers (web's left)")
	})
	t.Logf("edge removed %s after its last service's silo was deleted", time.Since(gone).Round(time.Second))
	t.Log("the shared NodeBalancer stayed while a service used it, and went with the last")
}

func testNodeBalancer(t *testing.T, web siloStatus, host machine, eventually func(string, time.Duration, func() (bool, string))) {
	t.Helper()
	eventually("the page through the NodeBalancer", 5*time.Minute, func() (bool, string) {
		body, err := get("http://" + web.Status.Service.LoadBalancerIP + "/")
		return strings.Contains(body, "served from a Linode volume"), fmt.Sprint(body, err)
	})
	t.Logf("http://%s/ answers from the volume through the NodeBalancer", web.Status.Service.LoadBalancerIP)
	// The node port isn't open to the internet, only to the NodeBalancer.
	if np := web.Status.Service.NodePorts; len(np) == 1 {
		if body, err := get(fmt.Sprintf("http://%s:%d/", host.Public, np[0].NodePort)); err == nil {
			t.Fatalf("node port %d answered from the internet: %q", np[0].NodePort, body)
		}
	}
}

// ipv4sOf resolves name's IPv4 addresses here (to grant them by address).
func ipv4sOf(t *testing.T, name string) []string {
	t.Helper()
	ips, err := net.LookupIP(name)
	var out []string
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			out = append(out, v4.String())
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s has no IPv4 address here: %v", name, err)
	}
	return out
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return l
}

func get(url string) (string, error) {
	// A new connection each time: a kept-alive one stays on the backend it was opened to (a
	// NodeBalancer doesn't cut a TCP stream when a port's backend leaves), so it would show what
	// the balancer did, not what it does now.
	c := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return string(b), err
}
