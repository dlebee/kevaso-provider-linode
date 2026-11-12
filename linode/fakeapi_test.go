package linode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/linode/linodego"
)

// fakeAPI is the slice of the Linode API v4 the provider uses, in memory.
type fakeAPI struct {
	mu        sync.Mutex
	next      int
	instances []map[string]any
	volumes   map[int]map[string]any
	nbs       map[int]map[string]any
	configs   map[int][]map[string]any // nodebalancer → configs (with "nodes")
	ips       map[string]map[string]any
	shared    map[int][]string // linode → IPs shared with it
	domains   map[int]map[string]any
	records   map[int][]map[string]any
	// hangDetach: detach requests to ignore (volume stays attached), like E032's first detach.
	hangDetach int
	calls      []string
}

func newFakeAPI(t *testing.T) (*fakeAPI, *Provider) {
	f := &fakeAPI{next: 100, volumes: map[int]map[string]any{}, nbs: map[int]map[string]any{}, configs: map[int][]map[string]any{},
		ips: map[string]map[string]any{}, shared: map[int][]string{}, domains: map[int]map[string]any{}, records: map[int][]map[string]any{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := linodego.NewClient(srv.Client())
	c.SetBaseURL(srv.URL)
	c.SetPollDelay(1)
	p := &Provider{API: &c, Tags: []string{"kevaso", "test"}, Poll: 1, AttachWait: 50e6, DetachWait: 50e6, DetachRetry: 1}
	return f, p
}

func (f *fakeAPI) id() int { f.next++; return f.next }

func (f *fakeAPI) addLinode(id int, region string, ipv4 ...string) {
	f.instances = append(f.instances, map[string]any{"id": id, "label": "l" + strconv.Itoa(id), "region": region, "ipv4": ipv4, "status": "running"})
	for _, ip := range ipv4 {
		f.ips[ip] = map[string]any{"address": ip, "linode_id": id, "region": region, "type": "ipv4", "public": !strings.HasPrefix(ip, "192.168.")}
	}
}

func list(items []map[string]any) map[string]any {
	if items == nil {
		items = []map[string]any{}
	}
	return map[string]any{"data": items, "page": 1, "pages": 1, "results": len(items)}
}

// matches applies an X-Filter of plain equalities.
func matches(r *http.Request, item map[string]any) bool {
	var f map[string]any
	if json.Unmarshal([]byte(r.Header.Get("X-Filter")), &f) != nil {
		return true
	}
	for k, v := range f {
		if fmt.Sprint(item[k]) != fmt.Sprint(v) {
			return false
		}
	}
	return true
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := strings.TrimPrefix(r.URL.Path, "/v4")
	f.calls = append(f.calls, r.Method+" "+p)
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	parts := strings.Split(strings.Trim(p, "/"), "/")
	num := func(i int) int { n, _ := strconv.Atoi(parts[i]); return n }
	reply := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
	notFound := func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		reply(map[string]any{"errors": []map[string]any{{"reason": "Not found"}}})
	}
	filtered := func(all []map[string]any) []map[string]any {
		var out []map[string]any
		for _, x := range all {
			if matches(r, x) {
				out = append(out, x)
			}
		}
		return out
	}
	values := func(m map[int]map[string]any) []map[string]any {
		var out []map[string]any
		for i := 0; i <= f.next; i++ {
			if x, ok := m[i]; ok {
				out = append(out, x)
			}
		}
		return out
	}
	switch {
	case r.Method == "GET" && p == "/linode/instances":
		reply(list(filtered(f.instances)))
	case r.Method == "GET" && len(parts) == 4 && parts[0] == "linode" && parts[3] == "ips":
		var shared []map[string]any
		for _, a := range f.shared[num(2)] {
			shared = append(shared, map[string]any{"address": a})
		}
		reply(map[string]any{"ipv4": map[string]any{"public": []any{}, "private": []any{}, "shared": shared}})
	case r.Method == "GET" && len(parts) == 3 && parts[0] == "networking" && parts[1] == "ips":
		if ip, ok := f.ips[parts[2]]; ok {
			reply(ip)
		} else {
			notFound()
		}
	case r.Method == "POST" && p == "/networking/ips/share":
		var ips []string
		for _, x := range body["ips"].([]any) {
			ips = append(ips, x.(string))
		}
		f.shared[int(body["linode_id"].(float64))] = ips
		reply(map[string]any{})
	case r.Method == "GET" && p == "/volumes":
		reply(list(filtered(values(f.volumes))))
	case r.Method == "POST" && p == "/volumes":
		id := f.id()
		v := map[string]any{"id": id, "label": body["label"], "region": body["region"], "size": body["size"], "status": "active",
			"linode_id": nil, "filesystem_path": "/dev/disk/by-id/scsi-0Linode_Volume_" + body["label"].(string), "tags": body["tags"]}
		f.volumes[id] = v
		reply(v)
	case len(parts) >= 2 && parts[0] == "volumes":
		v, ok := f.volumes[num(1)]
		if !ok {
			notFound()
			return
		}
		switch {
		case r.Method == "GET" && len(parts) == 2:
			reply(v)
		case r.Method == "DELETE":
			delete(f.volumes, num(1))
			reply(map[string]any{})
		case len(parts) == 3 && parts[2] == "attach":
			if v["linode_id"] != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(400)
				reply(map[string]any{"errors": []map[string]any{{"reason": "Volume is already attached"}}})
				return
			}
			v["linode_id"] = int(body["linode_id"].(float64))
			reply(v)
		case len(parts) == 3 && parts[2] == "detach":
			if f.hangDetach > 0 {
				f.hangDetach--
			} else {
				v["linode_id"] = nil
			}
			reply(map[string]any{})
		}
	case r.Method == "GET" && p == "/nodebalancers":
		reply(list(filtered(values(f.nbs))))
	case r.Method == "POST" && p == "/nodebalancers":
		id := f.id()
		nb := map[string]any{"id": id, "label": body["label"], "region": body["region"], "ipv4": fmt.Sprintf("172.233.0.%d", id%250), "tags": body["tags"]}
		f.nbs[id] = nb
		reply(nb)
	case len(parts) >= 2 && parts[0] == "nodebalancers":
		nb := num(1)
		if _, ok := f.nbs[nb]; !ok {
			notFound()
			return
		}
		switch {
		case r.Method == "DELETE" && len(parts) == 2:
			delete(f.nbs, nb)
			delete(f.configs, nb)
			reply(map[string]any{})
		case r.Method == "GET" && len(parts) == 3:
			reply(list(f.configs[nb]))
		case r.Method == "POST" && len(parts) == 3:
			c := map[string]any{"id": f.id(), "port": body["port"], "protocol": body["protocol"], "nodebalancer_id": nb}
			f.configs[nb] = append(f.configs[nb], c)
			reply(c)
		case len(parts) >= 4:
			cid := num(3)
			for i, c := range f.configs[nb] {
				if c["id"] != cid {
					continue
				}
				if r.Method == "DELETE" {
					f.configs[nb] = append(f.configs[nb][:i], f.configs[nb][i+1:]...)
					reply(map[string]any{})
					return
				}
				c["nodes"] = body["nodes"]
				c["port"] = body["port"]
				reply(c)
				return
			}
			notFound()
		}
	case r.Method == "GET" && p == "/domains":
		reply(list(values(f.domains)))
	case len(parts) >= 3 && parts[0] == "domains" && parts[2] == "records":
		d := num(1)
		switch {
		case r.Method == "GET":
			reply(list(f.records[d]))
		case r.Method == "POST":
			rec := map[string]any{"id": f.id(), "type": body["type"], "name": body["name"], "target": body["target"], "ttl_sec": body["ttl_sec"]}
			f.records[d] = append(f.records[d], rec)
			reply(rec)
		case len(parts) == 4:
			rid := num(3)
			for i, rec := range f.records[d] {
				if rec["id"] != rid {
					continue
				}
				if r.Method == "DELETE" {
					f.records[d] = append(f.records[d][:i], f.records[d][i+1:]...)
					reply(map[string]any{})
					return
				}
				for k, v := range body {
					rec[k] = v
				}
				reply(rec)
				return
			}
			notFound()
		}
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		reply(map[string]any{"errors": []map[string]any{{"reason": "fake: no " + r.Method + " " + p}}})
	}
}
