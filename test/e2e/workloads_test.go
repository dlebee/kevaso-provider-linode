//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// TestWorkloads proves, on real Linodes, workload features that were only run in-process before:
// port leases with the hosts' real nftables (design §7.2d), terminals and files (kvo exec -it,
// kvo ssh, kvo cp), plugin operators installed over SSH and systemd (ADR 0006), and moves between
// hosts, by hand and on host loss (design §0.3). One cluster serves them all: a control plane
// and two docker workers. Moves go last: they power a worker off.
//
//	KEVASO_LINODE_TOKEN_FILE=~/linode-token.txt go test -tags e2e -v -run TestWorkloads -timeout 120m ./test/e2e/
func TestWorkloads(t *testing.T) {
	tokenFile := os.Getenv("KEVASO_LINODE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("KEVASO_LINODE_TOKEN_FILE not set: this test creates real Linodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Minute)
	defer cancel()
	b := newBed(t, tokenFile, "kvt-wk-")
	t.Logf("run tag %s, region %s", b.tag, b.region)
	b.cleanupAtEnd(t)
	start := time.Now()
	c := newCluster(t, ctx, b)
	t.Logf("cluster up in %s", time.Since(start).Round(time.Second))
	only := os.Getenv("KEVASO_E2E_ONLY") // e.g. "leases,moves"
	for _, part := range []struct {
		name string
		f    func(*testing.T)
	}{{"leases", c.leases}, {"terminals", c.terminals}, {"operator", c.operator}, {"moves", c.moves}} {
		if only != "" && !strings.Contains(","+only+",", ","+part.name+",") {
			continue
		}
		s := time.Now()
		t.Run(part.name, part.f)
		t.Logf("%s: %s", part.name, time.Since(s).Round(time.Second))
	}
	t.Logf("Linodes up for %s", time.Since(start).Round(time.Minute))
}

// pyServer is a silo's program: HTTP on web, and on debug a line server that says hello and
// echoes what it's sent.
func pyServer(web, debug int) string {
	script := fmt.Sprintf(`import socket, threading, http.server
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(s):
        s.send_response(200); s.end_headers(); s.wfile.write(b"web answers\n")
    def log_message(s, *a): pass
def echo(c):
    try:
        c.sendall(b"hello\n")
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(d)
    except Exception: pass
    c.close()
def debug():
    l = socket.socket(); l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); l.bind(("", %d)); l.listen(16)
    while True:
        c, _ = l.accept(); threading.Thread(target=echo, args=(c,), daemon=True).start()
threading.Thread(target=debug, daemon=True).start()
http.server.ThreadingHTTPServer(("", %d), H).serve_forever()
`, debug, web)
	q, _ := json.Marshal(script)
	return string(q)
}

const pyImage = "docker.io/library/python:3.13-alpine"

// banner dials addr and reads the debug server's hello: "" and an error when it's closed to us.
func banner(addr string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 4*time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	return bufio.NewReader(conn).ReadString('\n')
}

type lease struct {
	Spec   struct{ Silo, Port string } `json:"spec"`
	Status struct {
		Phase       string     `json:"phase"`
		OpenedAt    *time.Time `json:"openedAt"`
		Until       *time.Time `json:"until"`
		RequestedBy string     `json:"requestedBy"`
		ApprovedBy  string     `json:"approvedBy"`
		Reason      string     `json:"reason"`
		Pending     *struct {
			Approval string `json:"approval"`
		} `json:"pending"`
	} `json:"status"`
}

func (c *fleet) lease(t *testing.T, silo, port string) lease {
	t.Helper()
	out, err := c.kvoe("get", "leases", "-o", "json")
	if err != nil {
		t.Fatalf("kvo get leases: %v %s", err, out)
	}
	var l []lease // a list is a JSON array
	if err := json.Unmarshal([]byte(out), &l); err != nil {
		t.Fatalf("leases: %v\n%s", err, out)
	}
	for _, x := range l {
		if x.Spec.Silo == silo && x.Spec.Port == port {
			return x
		}
	}
	return lease{}
}

// ── 1. Port leases ─────────────────────────────────────────────────────────────────────────────

func (c *fleet) leases(t *testing.T) {
	type svc struct {
		name, lease string
		web, debug  int
	}
	silos := []svc{
		{"api", "{ ports: [debug], max: 2m }", 8080, 9229},
		{"gated", "{ ports: [debug], max: 5m, approval: { required: true } }", 8081, 9230},
		{"narrow", `{ ports: [debug], max: 5m, allowFrom: ["203.0.113.0/24"] }`, 8082, 9231},
	}
	c.fresh(t, "silo", "api", "gated", "narrow")
	for _, s := range silos {
		c.apply(t, fmt.Sprintf(`apiVersion: kevaso/v1
kind: Silo
metadata: { name: %s }
spec:
  runtime: docker
  image: %s
  resources: { memory: 64Mi }
  command: [python3, -u, -c, %s]
  service:
    ports: [{ name: web, port: %d }, { name: debug, port: %d }]
    expose: hostPort
    lease: %s
`, s.name, pyImage, pyServer(s.web, s.debug), s.web, s.debug, s.lease))
	}
	addr := map[string]string{}
	web := map[string]string{}
	for _, s := range silos {
		info := c.running(t, s.name, 6*time.Minute)
		ip := c.host(info.Status.Host).Public
		addr[s.name] = fmt.Sprintf("%s:%d", ip, s.debug)
		web[s.name] = fmt.Sprintf("http://%s:%d/", ip, s.web)
		t.Logf("%s on %s: web %s, debug %s", s.name, info.Status.Host, web[s.name], addr[s.name])
	}
	webUp := func(when string) {
		t.Helper()
		for _, s := range silos {
			var body string
			var err error
			for i := 0; i < 5; i++ { // the first may come while the container starts
				if body, err = get(web[s.name]); err == nil && strings.Contains(body, "web answers") {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if !strings.Contains(body, "web answers") {
				t.Fatalf("%s: %s's web port: %q %v", when, s.name, body, err)
			}
		}
	}
	isOpen := func(name string) (bool, string) {
		got, err := banner(addr[name])
		return got == "hello\n", fmt.Sprintf("%q %v", got, err)
	}
	staysClosed := func(name, when string, d time.Duration) {
		t.Helper()
		for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(2 * time.Second) {
			if ok, s := isOpen(name); ok {
				t.Fatalf("%s: %s's debug port answered: %s", when, name, s)
			}
		}
	}
	opens := func(name, when string) {
		t.Helper()
		c.eventually(t, when+": "+name+"'s debug port open to us", 45*time.Second, func() (bool, string) { return isOpen(name) })
	}
	closes := func(name, when string, d time.Duration) {
		t.Helper()
		c.eventually(t, when+": "+name+"'s debug port closed to us", d, func() (bool, string) {
			ok, s := isOpen(name)
			return !ok, s
		})
	}

	// Closed by default; the web ports aren't leased.
	webUp("at first")
	for _, s := range silos {
		staysClosed(s.name, "closed by default", 6*time.Second)
	}
	t.Log("every debug port closed by default; every web port answers")

	// kvo open for 60s: it opens; a connection held open is cut at the end, and new ones refused.
	out := c.mustKvo(t, "open", "silo", "api", "--port", "debug", "--for", "60s")
	if !strings.Contains(out, "open until") {
		t.Fatalf("kvo open: %s", out)
	}
	opens("api", "kvo open --for 60s")
	l := c.lease(t, "api", "debug")
	if l.Status.Phase != "Open" || l.Status.Until == nil {
		t.Fatalf("lease: %+v", l.Status)
	}
	until := *l.Status.Until
	conn, err := net.DialTimeout("tcp", addr["api"], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rd := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if hello, err := rd.ReadString('\n'); hello != "hello\n" {
		t.Fatalf("held connection: %q %v", hello, err)
	}
	var cut time.Time
	for i := 0; time.Now().Before(until.Add(45 * time.Second)); i++ {
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		msg := fmt.Sprintf("ping %d\n", i)
		_, werr := conn.Write([]byte(msg))
		got, rerr := rd.ReadString('\n')
		if werr != nil || rerr != nil || got != msg {
			cut = time.Now()
			t.Logf("held connection cut %s after the lease's end (%v %v %q)", cut.Sub(until).Round(time.Second), werr, rerr, got)
			break
		}
		if i%10 == 0 {
			webUp("while the lease is open")
		}
		time.Sleep(time.Second)
	}
	_ = conn.Close()
	switch {
	case cut.IsZero():
		t.Fatalf("a connection held open still worked 45s after the lease ended at %s", until.Format(time.TimeOnly))
	case cut.Before(until.Add(-3 * time.Second)):
		t.Fatalf("the connection was cut at %s, before the lease's end at %s", cut.Format(time.TimeOnly), until.Format(time.TimeOnly))
	}
	closes("api", "after its end", 20*time.Second)
	staysClosed("api", "after its end", 6*time.Second)
	webUp("after the lease ended")
	t.Logf("a 60s lease opened the port; at its end the held connection was cut and new ones refused")

	// kvo close ends it early, and cuts what's held too.
	c.mustKvo(t, "open", "silo", "api", "--port", "debug", "--for", "2m")
	opens("api", "kvo open --for 2m")
	conn, err = net.DialTimeout("tcp", addr["api"], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rd = bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = rd.ReadString('\n')
	if out := c.mustKvo(t, "close", "silo", "api", "--port", "debug"); !strings.Contains(out, "closed") {
		t.Fatalf("kvo close: %s", out)
	}
	closedAt := time.Now()
	closes("api", "kvo close", 20*time.Second)
	held := true
	for i := 0; i < 15 && held; i++ {
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, werr := conn.Write([]byte("still?\n"))
		got, rerr := rd.ReadString('\n')
		held = werr == nil && rerr == nil && got == "still?\n"
		time.Sleep(time.Second)
	}
	_ = conn.Close()
	if held {
		t.Fatal("kvo close left a held connection working")
	}
	if l := c.lease(t, "api", "debug"); l.Status.Phase != "Closed" {
		t.Fatalf("after kvo close: %+v", l.Status)
	}
	t.Logf("kvo close shut it %s early, held connection cut", time.Until(closedAt.Add(2*time.Minute)).Round(time.Second))

	// An ask longer than the silo's max (2m) is cut to the max, and says so.
	cmd := c.cmd("", nil, c.kvo, "open", "silo", "api", "-n", "e2e", "--port", "debug", "--for", "10m")
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("kvo open --for 10m: %v %s %s", err, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "more than its lease's max") {
		t.Fatalf("no word of the max: stdout %q stderr %q", stdout.String(), stderr.String())
	}
	l = c.lease(t, "api", "debug")
	if l.Status.Until == nil || l.Status.OpenedAt == nil {
		t.Fatalf("lease: %+v", l.Status)
	}
	if got := l.Status.Until.Sub(*l.Status.OpenedAt); got != 2*time.Minute {
		t.Fatalf("asked 10m with max 2m: open for %s", got)
	}
	opens("api", "kvo open --for 10m (cut to 2m)")
	c.mustKvo(t, "close", "silo", "api", "--port", "debug")
	closes("api", "closed again", 20*time.Second)
	t.Log("an ask for 10m got the silo's max, 2m, and kvo said so")

	// approval: required. Someone who may open but not approve (a service account bound to
	// editor) asks; the port stays closed until an admin says yes. A no keeps it closed.
	c.apply(t, `apiVersion: kevaso/v1
kind: ServiceAccount
metadata: { name: opener }
---
apiVersion: kevaso/v1
kind: RoleBinding
metadata: { name: opener }
roleRef: { kind: ClusterRole, name: editor }
subjects: [{ kind: ServiceAccount, name: opener }]
`)
	token := strings.TrimSpace(c.mustKvo(t, "create", "token", "opener", "--duration", "1h"))
	asOpener := func(args ...string) (string, error) {
		out, err := c.cmd("", []string{"KVO_TOKEN=" + token}, c.kvo, inE2E(args)...).CombinedOutput()
		return string(out), err
	}
	ask := func() string {
		t.Helper()
		out, err := asOpener("open", "silo", "gated", "--port", "debug", "--for", "60s", "--no-wait")
		if err != nil || !strings.Contains(out, "waiting for a yes: kvo approve ") {
			t.Fatalf("opener's ask: %v %s", err, out)
		}
		f := strings.Fields(out[strings.Index(out, "kvo approve ")+len("kvo approve "):])
		return f[0]
	}
	if out, err := asOpener("approve", "nothing"); err == nil {
		t.Fatalf("the service account answered an approval: %s", out)
	}
	appr := ask()
	if l := c.lease(t, "gated", "debug"); l.Status.Pending == nil || l.Status.Pending.Approval != appr {
		t.Fatalf("no pending ask: %+v", l.Status)
	}
	staysClosed("gated", "waiting for a yes", 10*time.Second)
	if out, err := asOpener("approve", appr); err == nil {
		t.Fatalf("the asker approved its own ask: %s", out)
	}
	c.mustKvo(t, "approve", appr)
	opens("gated", "approved")
	if l := c.lease(t, "gated", "debug"); l.Status.ApprovedBy == "" || l.Status.Phase != "Open" {
		t.Fatalf("approved lease: %+v", l.Status)
	}
	c.mustKvo(t, "close", "silo", "gated", "--port", "debug")
	closes("gated", "closed", 20*time.Second)
	appr = ask()
	c.mustKvo(t, "deny", appr, "--reason", "not now")
	staysClosed("gated", "denied", 10*time.Second)
	if l := c.lease(t, "gated", "debug"); l.Status.Phase != "Denied" {
		t.Fatalf("denied lease: %+v", l.Status)
	}
	t.Log("approval required: closed until kvo approve; the asker couldn't answer; a deny kept it closed")

	// lease.allowFrom that leaves this machine out: open, but not to us; the host's rule admits
	// only that range.
	c.mustKvo(t, "open", "silo", "narrow", "--port", "debug", "--for", "60s")
	if l := c.lease(t, "narrow", "debug"); l.Status.Phase != "Open" {
		t.Fatalf("narrow: %+v", l.Status)
	}
	ninfo, _ := c.silo("narrow")
	c.eventually(t, "narrow's rule from 203.0.113.0/24 on its host", 30*time.Second, func() (bool, string) {
		out, _ := c.b.ssh(c.ctx, c.host(ninfo.Status.Host), "nft list ruleset | grep 'dport 9231'")
		return strings.Contains(out, "203.0.113.0/24"), out
	})
	staysClosed("narrow", "open, but only from 203.0.113.0/24", 10*time.Second)
	webUp("narrow open from elsewhere")
	c.mustKvo(t, "close", "silo", "narrow", "--port", "debug")
	t.Log("lease.allowFrom without us: open on the host for that range only, closed to us")

	// From inside the silo, through its broker socket: kvo-guest (the kevasod binary under that
	// name, copied in with kvo cp).
	for _, name := range []string{"api", "gated"} {
		c.mustKvo(t, "cp", c.kevasod, "silo/"+name+":/tmp/kvo-guest")
		c.mustKvo(t, "exec", "silo", name, "--", "chmod", "0755", "/tmp/kvo-guest")
	}
	if out := c.mustKvo(t, "exec", "silo", "api", "--", "/tmp/kvo-guest", "port", "open", "debug", "--for", "45s"); out == "" {
		t.Log("kvo-guest port open: (no output)")
	}
	opens("api", "kvo-guest port open")
	if l := c.lease(t, "api", "debug"); !strings.Contains(l.Status.RequestedBy, "api") {
		t.Logf("api's own lease, requested by %q", l.Status.RequestedBy)
	}
	c.mustKvo(t, "exec", "silo", "api", "--", "/tmp/kvo-guest", "port", "close", "debug")
	closes("api", "kvo-guest port close", 20*time.Second)
	type res struct {
		out string
		err error
	}
	done := make(chan res, 1)
	go func() {
		out, err := c.kvoe("exec", "silo", "gated", "--", "/tmp/kvo-guest", "port", "open", "debug", "--for", "45s", "--wait", "2m")
		done <- res{out, err}
	}()
	var pending string
	c.eventually(t, "gated's own ask pending", 30*time.Second, func() (bool, string) {
		l := c.lease(t, "gated", "debug")
		if l.Status.Pending != nil {
			pending = l.Status.Pending.Approval
		}
		return pending != "", fmt.Sprintf("%+v", l.Status)
	})
	staysClosed("gated", "its own ask, waiting", 6*time.Second)
	c.mustKvo(t, "approve", pending)
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("kvo-guest port open --wait: %v %s", r.err, r.out)
		}
	case <-time.After(time.Minute):
		t.Fatal("kvo-guest port open --wait didn't return after the yes")
	}
	opens("gated", "its own ask approved")
	c.mustKvo(t, "close", "silo", "gated", "--port", "debug")
	closes("gated", "closed", 20*time.Second)
	webUp("at the end")
	t.Log("from inside: kvo-guest opened and closed api's port; gated's own ask waited for kvo approve")
	for _, s := range silos {
		c.mustKvo(t, "delete", "silo", s.name)
	}
}

// ── 3. Terminals and files ─────────────────────────────────────────────────────────────────────

func (c *fleet) terminals(t *testing.T) {
	c.fresh(t, "silo", "term")
	c.apply(t, `apiVersion: kevaso/v1
kind: Silo
metadata: { name: term }
spec:
  runtime: docker
  image: `+pyImage+`
  resources: { memory: 96Mi }
  command: [python3, -c, "import time\nwhile True: time.sleep(3600)"]
`)
	info := c.running(t, "term", 6*time.Minute)
	host := c.host(info.Status.Host)
	c.ptyExec(t)
	c.ptySSH(t)
	c.copies(t)
	c.killedClient(t, host)
	c.mustKvo(t, "delete", "silo", "term")
}
