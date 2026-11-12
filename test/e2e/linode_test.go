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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

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
			PublicIP  string `json:"publicIP"`
			NodePorts []struct {
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
	b := newBed(t, tokenFile)
	t.Logf("run tag %s, region %s", b.tag, b.region)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		if os.Getenv("KEVASO_E2E_KEEP") != "" && t.Failed() {
			t.Logf("KEEPING everything tagged %s", b.tag)
			return
		}
		swept := b.sweep(ctx)
		t.Logf("swept: %s", strings.Join(swept, ", "))
		for i := 0; i < 20; i++ {
			left := b.remaining(ctx)
			if len(left) == 0 {
				t.Logf("nothing tagged %s is left in the account", b.tag)
				return
			}
			time.Sleep(15 * time.Second)
			b.sweep(ctx)
		}
		t.Errorf("still tagged %s after cleanup: %v", b.tag, b.remaining(ctx))
	})

	// Binaries: the CLIs for here, kevasod and the plugin for the Linodes.
	bin := t.TempDir()
	build := func(out, pkg, goos string) string {
		p := filepath.Join(bin, out)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", p, pkg)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if goos == "linux" {
			cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
		}
		if o, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, o)
		}
		return p
	}
	kvoadm := build("kvoadm", "kevaso.com/core/cmd/kvoadm", runtime.GOOS)
	kvo := build("kvo", "kevaso.com/core/cmd/kvo", runtime.GOOS)
	kevasod := build("kevasod", "kevaso.com/core/cmd/kevasod", "linux")
	plugin := build("kevaso-provider-linode", "kevaso.com/provider-linode/cmd/kevaso-provider-linode", "linux")
	home := t.TempDir()
	env := append(os.Environ(), "HOME="+home, "KVO_CONFIG="+filepath.Join(home, ".kevaso", "config.yaml"), "KVO_SERVER=")
	sh := func(stdin string, name string, args ...string) (string, error) {
		c := exec.CommandContext(ctx, name, args...)
		c.Env, c.Stdin = env, strings.NewReader(stdin)
		out, err := c.CombinedOutput()
		return string(out), err
	}
	must := func(stdin string, name string, args ...string) string {
		t.Helper()
		out, err := sh(stdin, name, args...)
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
		}
		return out
	}
	ssh := []string{"--ssh-key", b.key, "--accept-host-key-on-first-use"}
	eventually := func(what string, d time.Duration, f func() (bool, string)) {
		t.Helper()
		deadline, last := time.Now().Add(d), ""
		for time.Now().Before(deadline) {
			ok, s := f()
			if ok {
				return
			}
			last = s
			time.Sleep(5 * time.Second)
		}
		t.Fatalf("%s: not in %v; last: %s", what, d, last)
	}
	silo := func(name string) siloStatus {
		var s siloStatus
		out, err := sh("", kvo, "get", "silo", name, "-n", "e2e", "-o", "json")
		if err == nil {
			_ = json.Unmarshal([]byte(out), &s)
		}
		return s
	}

	// 1. Three Linodes, and a fourth whose IPv4 the shared-IP silo borrows (extra IPv4s need a
	// support ticket, E031): it runs nothing.
	type made struct {
		role string
		m    machine
	}
	ch := make(chan made, 4)
	for _, role := range []string{"cp", "w1", "w2", "ip"} {
		go func() { ch <- made{role, b.create(ctx, role)} }()
	}
	ms := map[string]machine{}
	for i := 0; i < 4; i++ {
		x := <-ch
		ms[x.role] = x.m
	}
	if t.Failed() {
		t.FailNow()
	}
	cp, w1, w2, donor := ms["cp"], ms["w1"], ms["w2"], ms["ip"]
	t.Cleanup(func() { // runs before the sweep: what was going on, if it failed
		if !t.Failed() {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for name, m := range map[string]machine{"cp": cp, "w1": w1, "w2": w2} {
			out, _ := b.ssh(dctx, m, "ls -la /run/kevaso-plugins/ /run/kevaso-plugins/* 2>&1; systemctl --no-pager status kevaso-plugin-linode 2>&1 | head -5; "+
				"journalctl -u kevaso-plugin-linode -n 30 --no-pager 2>&1; journalctl -u kevasod -n 60 --no-pager 2>&1 | grep -iv 'heartbeat' | tail -40")
			t.Logf("── %s ──\n%s", name, out)
		}
		for _, args := range [][]string{{"get", "hosts", "-o", "yaml"}, {"get", "silos", "-A", "-o", "yaml"}} {
			c := exec.CommandContext(dctx, kvo, args...)
			c.Env = env
			out, _ := c.CombinedOutput()
			t.Logf("── kvo %s ──\n%s", strings.Join(args, " "), out)
		}
	})

	// 2. The cluster: kvoadm init, two workers with docker.
	out := must("", kvoadm, append([]string{"init", "root@" + cp.Public, "--kevasod", kevasod, "--name", "cp", "--cluster", "e2e",
		"--api-server", "https://" + cp.Public + ":6443"}, ssh...)...)
	if !regexp.MustCompile(`KVR1-`).MatchString(out) {
		t.Fatalf("no recovery key printed:\n%s", out)
	}
	for _, w := range []struct {
		name string
		m    machine
	}{{"w1", w1}, {"w2", w2}} {
		t.Log(must("", kvoadm, append([]string{"host", "add", "root@" + w.m.Public, "--kevasod", kevasod, "--name", w.name,
			"--install", "worker", "--runtime", "docker", "--region", b.region}, ssh...)...))
	}

	// 3. The plugin: controller side on the control plane (with the token), node side on the workers.
	cfgFile := filepath.Join(t.TempDir(), "linode.json")
	_ = os.WriteFile(cfgFile, []byte(fmt.Sprintf(`{"tags": [%q]}`, b.tag)), 0o644)
	t.Log(must("", kvoadm, append([]string{"plugin", "install", "root@" + cp.Public, "--name", "linode", "--binary", plugin,
		"--credential", "token=" + tokenFile, "--config", cfgFile}, ssh...)...))
	nodeCfg := filepath.Join(t.TempDir(), "node.json")
	_ = os.WriteFile(nodeCfg, []byte(fmt.Sprintf(`{"region": %q}`, b.region)), 0o644)
	for _, w := range []machine{w1, w2} {
		t.Log(must("", kvoadm, append([]string{"plugin", "install", "root@" + w.Public, "--name", "linode", "--binary", plugin,
			"--side", "node", "--config", nodeCfg}, ssh...)...))
	}
	// The token is on the control plane only, readable by root only.
	if out, _ := b.ssh(ctx, cp, "stat -c '%a %U' /etc/kevaso/plugins/linode/token"); strings.TrimSpace(out) != "600 root" {
		t.Fatalf("token file: %q", out)
	}
	if out, _ := b.ssh(ctx, w1, "ls /etc/kevaso/plugins/linode/"); strings.Contains(out, "token") {
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

	// 4. A container silo with a Block Storage volume behind a NodeBalancer, and an Ingress class
	// publishing its host name in Linode DNS.
	zone := b.tag + ".kevaso.com"
	dns := true
	if _, err := b.api.CreateDomain(ctx, linodego.DomainCreateOptions{Domain: zone, Type: linodego.DomainTypeMaster,
		SOAEmail: "hostmaster@kevaso.com", Tags: []string{b.tag}}); err != nil {
		var le *linodego.Error
		if !errors.As(err, &le) || (le.Code != 401 && le.Code != 403) {
			t.Fatalf("test zone: %v", err)
		}
		dns = false
		t.Logf("SKIPPING the DNS part: this token can't use Domains (%v); give it Domains read/write to test it", err)
	}
	nb := true
	if _, err := b.api.ListNodeBalancers(ctx, nil); err != nil {
		var le *linodego.Error
		if !errors.As(err, &le) || (le.Code != 401 && le.Code != 403) {
			t.Fatalf("NodeBalancers: %v", err)
		}
		nb, dns = false, false // the Ingress's address would be the NodeBalancer's
		t.Logf("SKIPPING the NodeBalancer part: this token can't use NodeBalancers (%v); give it NodeBalancers read/write to test it", err)
	}
	exposure := `
  service:
    ports: [{ name: http, port: 80 }]
    expose: loadBalancer
    loadBalancer: { class: linode-nodebalancer }
`
	if !nb {
		exposure = `
  service:
    ports: [{ name: http, port: 80 }]
`
	}
	ingress := `---
apiVersion: kevaso/v1
kind: IngressClass
metadata: { name: linode }
spec:
  controller: { silo: web, namespace: e2e }
  dns: { provider: linode, ttl: 30 }
---
apiVersion: kevaso/v1
kind: Ingress
metadata: { name: shop, namespace: e2e }
spec:
  ingressClass: linode
  hosts: [shop.` + zone + `]
  tls: { mode: none }
  rules: [{ path: /, silo: { name: web, port: http } }]
`
	if !dns {
		ingress = ""
	}
	must(`apiVersion: kevaso/v1
kind: Namespace
metadata: { name: e2e }
---
apiVersion: kevaso/v1
kind: Silo
metadata: { name: web, namespace: e2e }
spec:
  runtime: docker
  image: docker.io/library/nginx:1.27-alpine
  resources: { cpu: 1, memory: 256Mi }
  volumes:
    - { name: site, class: linode-block, size: 10Gi, mountPath: /usr/share/nginx/html }
  init:
    steps:
      - name: page
        run: |
          [ -f /usr/share/nginx/html/index.html ] || echo "served from a Linode volume" > /usr/share/nginx/html/index.html`+exposure+ingress, kvo, "apply", "-f", "-")
	var web siloStatus
	eventually("web running with its volume and NodeBalancer", 10*time.Minute, func() (bool, string) {
		web = silo("web")
		ok := web.Status.Phase == "Running" && (web.Status.Service.PublicIP != "" || !nb) && len(web.Status.Volumes) == 1 && web.Status.Volumes[0].Mounted
		return ok, fmt.Sprintf("%+v", web.Status)
	})
	t.Logf("web on %s, NodeBalancer %s, volume %s at %s", web.Status.Host, web.Status.Service.PublicIP, web.Status.Volumes[0].Handle, web.Status.Volumes[0].Device)
	host := map[string]machine{"w1": w1, "w2": w2}[web.Status.Host]
	if out, _ := b.ssh(ctx, host, "findmnt -n -o SOURCE,FSTYPE /var/lib/kevaso/volumes/"+web.Metadata.UID+"/site"); !strings.Contains(out, "ext4") {
		t.Fatalf("volume mount on %s: %q", web.Status.Host, out)
	}
	if got := must("", kvo, "exec", "silo", "web", "-n", "e2e", "--", "cat", "/usr/share/nginx/html/index.html"); !strings.Contains(got, "served from a Linode volume") {
		t.Fatalf("the page on the volume: %q", got)
	}
	t.Log("the init step wrote the page onto the Linode volume")
	if nb {
		testNodeBalancer(t, web, host, eventually)
	}
	if dns {
		eventually("shop."+zone+" in Linode DNS", 20*time.Minute, func() (bool, string) {
			addrs, err := lookup(ctx, "shop."+zone)
			return err == nil && len(addrs) == 1 && addrs[0] == web.Status.Service.PublicIP, fmt.Sprint(addrs, err)
		})
		t.Logf("shop.%s → %s on ns1.linode.com", zone, web.Status.Service.PublicIP)
		must("", kvo, "delete", "ingress", "shop", "-n", "e2e")
	}

	// 5. Delete it all: the record, the NodeBalancer and the volume go with them.
	must("", kvo, "delete", "silo", "web", "-n", "e2e")
	eventually("web, its NodeBalancer, volume and record gone", 10*time.Minute, func() (bool, string) {
		_, err := sh("", kvo, "get", "silo", "web", "-n", "e2e")
		left := b.remaining(ctx)
		var cloud []string
		for _, l := range left {
			if !strings.HasPrefix(l, "Linode ") && !strings.HasPrefix(l, "domain ") {
				cloud = append(cloud, l)
			}
		}
		recs := 0
		if ds, _ := b.api.ListDomains(ctx, nil); len(ds) > 0 {
			for _, d := range ds {
				if d.Domain == zone {
					rs, _ := b.api.ListDomainRecords(ctx, d.ID, nil)
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

	// 6. A shared IP that follows its silo: the donor's IPv4, announced over BGP by the silo's host;
	// that host goes down, the silo is recreated on the other one, and the address answers there.
	must(`apiVersion: kevaso/v1
kind: Silo
metadata: { name: edge, namespace: e2e }
spec:
  runtime: docker
  image: docker.io/library/nginx:1.27-alpine
  resources: { cpu: 1, memory: 256Mi }
  availability: { onHostLoss: Recreate }
  command: [sh, -c, "echo edge answers > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"]
  service:
    ports: [{ name: http, port: 80 }]
    expose: loadBalancer
    loadBalancer: { class: linode-shared-ip, address: `+donor.Public+` }
`, kvo, "apply", "-f", "-")
	var edge siloStatus
	eventually("edge running on the shared IP", 8*time.Minute, func() (bool, string) {
		edge = silo("edge")
		return edge.Status.Phase == "Running" && edge.Status.Service.PublicIP == donor.Public, fmt.Sprintf("%+v", edge.Status)
	})
	first := edge.Status.Host
	eventually("http://"+donor.Public+"/ answered by edge on "+first, 5*time.Minute, func() (bool, string) {
		body, err := get("http://" + donor.Public + "/")
		return strings.Contains(body, "edge answers"), fmt.Sprint(body, err)
	})
	t.Logf("%s answers from edge on %s (announced over BGP)", donor.Public, first)
	down := map[string]machine{"w1": w1, "w2": w2}[first]
	if err := b.api.ShutdownInstance(ctx, down.ID); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s shut down", first)
	start := time.Now()
	eventually("edge recreated on the other worker", 6*time.Minute, func() (bool, string) {
		edge = silo("edge")
		return edge.Status.Host != "" && edge.Status.Host != first && edge.Status.Phase == "Running", fmt.Sprintf("%+v", edge.Status)
	})
	eventually("the shared IP answering from "+edge.Status.Host, 3*time.Minute, func() (bool, string) {
		body, err := get("http://" + donor.Public + "/")
		return strings.Contains(body, "edge answers"), fmt.Sprint(body, err)
	})
	t.Logf("%s answers again, from edge on %s, %s after %s went down", donor.Public, edge.Status.Host, time.Since(start).Round(time.Second), first)
	must("", kvo, "delete", "silo", "edge", "-n", "e2e")
	eventually("edge's shares removed", 5*time.Minute, func() (bool, string) {
		ips, err := b.api.GetInstanceIPAddresses(ctx, map[string]machine{"w1": w1, "w2": w2}[edge.Status.Host].ID)
		if err != nil || ips.IPv4 == nil {
			return false, fmt.Sprint(err)
		}
		return len(ips.IPv4.Shared) == 0, fmt.Sprint(len(ips.IPv4.Shared))
	})
}

func testNodeBalancer(t *testing.T, web siloStatus, host machine, eventually func(string, time.Duration, func() (bool, string))) {
	t.Helper()
	eventually("the page through the NodeBalancer", 5*time.Minute, func() (bool, string) {
		body, err := get("http://" + web.Status.Service.PublicIP + "/")
		return strings.Contains(body, "served from a Linode volume"), fmt.Sprint(body, err)
	})
	t.Logf("http://%s/ answers from the volume through the NodeBalancer", web.Status.Service.PublicIP)
	// The node port isn't open to the internet, only to the NodeBalancer.
	if np := web.Status.Service.NodePorts; len(np) == 1 {
		if body, err := get(fmt.Sprintf("http://%s:%d/", host.Public, np[0].NodePort)); err == nil {
			t.Fatalf("node port %d answered from the internet: %q", np[0].NodePort, body)
		}
	}
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return l
}

func get(url string) (string, error) {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return string(b), err
}
