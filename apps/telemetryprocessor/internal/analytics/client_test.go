package analytics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

func events(n int) []model.AnalyticsEvent {
	out := make([]model.AnalyticsEvent, n)
	for i := range out {
		out[i] = model.AnalyticsEvent{ID: int64(i + 1), Category: "T1566"}
	}
	return out
}

func fastOpts() Options {
	return Options{
		Timeout:      2 * time.Second,
		RateRequests: 1000,
		RateWindow:   time.Second,
		MaxRetries:   3,
	}
}

func TestSendBatchCountsIngested(t *testing.T) {
	var (
		mu       sync.Mutex
		batchLen []int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "k" {
			t.Errorf("missing auth header")
		}
		var batch []model.AnalyticsEvent
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode: %v", err)
		}
		if len(batch) > UpstreamMaxBatchSize {
			t.Errorf("batch of %d exceeds upstream max %d", len(batch), UpstreamMaxBatchSize)
		}
		mu.Lock()
		batchLen = append(batchLen, len(batch))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", fastOpts())
	got, err := c.SendBatch(context.Background(), events(20))
	if err != nil {
		t.Fatal(err)
	}
	if got != 20 {
		t.Fatalf("ingested = %d, want 20", got)
	}
	if len(batchLen) != 1 || batchLen[0] != 20 {
		t.Fatalf("unexpected requests: %v", batchLen)
	}
}

func TestSendBatchRejectsOversizedBatch(t *testing.T) {
	c := NewClient("http://unused", "k", fastOpts())
	if _, err := c.SendBatch(context.Background(), events(21)); err == nil {
		t.Fatal("expected an error for a batch above the upstream maximum")
	}
}

func TestSendRetriesOn429(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: 3})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", fastOpts())
	got, err := c.SendBatch(context.Background(), events(3))
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("ingested = %d, want 3", got)
	}
	if calls != 2 {
		t.Fatalf("expected 1 x 429 + 1 success = 2 calls, got %d", calls)
	}
}

func TestSendRateLimitThrottles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer srv.Close()

	// The upstream limit is 1 *request* per window (see openapi.json), so two
	// requests must straddle at least one full window.
	opts := fastOpts()
	opts.RateRequests = 1
	opts.RateWindow = 200 * time.Millisecond
	c := NewClient(srv.URL, "k", opts)

	start := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := c.SendBatch(context.Background(), events(20)); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Fatalf("expected rate limiting to add ~200ms, only took %v", elapsed)
	}
}

// The upstream limit counts requests, not items: a short trailing batch must
// still consume a whole window, otherwise the processor out-paces the limit
// and earns 429s.
func TestSendRateLimitsShortBatchesPerRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer srv.Close()

	opts := fastOpts()
	opts.RateRequests = 1
	opts.RateWindow = 200 * time.Millisecond
	c := NewClient(srv.URL, "k", opts)

	start := time.Now()
	for _, n := range []int{20, 1} {
		if _, err := c.SendBatch(context.Background(), events(n)); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Fatalf("short batch skipped the rate limiter: 2 requests took only %v", elapsed)
	}
}
