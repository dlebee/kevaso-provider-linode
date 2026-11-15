package linode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kevaso.com/core/sdk/kplugin"
)

// K-041: a call to an API that doesn't answer gives up within the call's deadline and says so; a
// rate-limited API is asked at most Retries more times.
func TestCallsGiveUpInBoundedTime(t *testing.T) {
	var hits atomic.Int64
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-r.Context().Done()
	}))
	defer hang.Close()
	c := NewClient("t")
	c.SetBaseURL(hang.URL)
	p := &Provider{API: c, Tags: []string{"kevaso"}, CallTimeout: 300 * time.Millisecond}
	start := time.Now()
	_, err := p.EnsureVolume(context.Background(), kplugin.Volume{Class: ClassBlock, ID: "u/d", Name: "kvo-a-b-c-1", SizeBytes: 1, Region: "ca-central"})
	if took := time.Since(start); err == nil || took > 3*time.Second || !strings.Contains(err.Error(), "gave up") {
		t.Fatalf("a silent API: %v after %v", err, took)
	}
	t.Logf("silent API: %v", err)

	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"errors":[{"reason":"Too Many Requests"}]}`))
	}))
	defer busy.Close()
	c = NewClient("t")
	c.SetBaseURL(busy.URL)
	c.SetRetryWaitTime(time.Millisecond)
	c.SetRetryMaxWaitTime(10 * time.Millisecond)
	p = &Provider{API: c, Tags: []string{"kevaso"}}
	hits.Store(0)
	start = time.Now()
	_, err = p.EnsureVolume(context.Background(), kplugin.Volume{Class: ClassBlock, ID: "u/d", Name: "kvo-a-b-c-1", SizeBytes: 1, Region: "ca-central"})
	if err == nil || hits.Load() != 1+Retries {
		t.Fatalf("a rate-limited API: %v, asked %d times (want %d)", err, hits.Load(), 1+Retries)
	}
	t.Logf("rate-limited API, asked %d times in %v: %v", hits.Load(), time.Since(start).Round(time.Millisecond), err)
}
