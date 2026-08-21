package enrichment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

func testOpts() Options {
	return Options{
		Timeout:          2 * time.Second,
		MaxAttempts:      4,
		BackoffBase:      time.Millisecond,
		BackoffMax:       2 * time.Millisecond,
		BreakerThreshold: 3,
		BreakerCooldown:  50 * time.Millisecond,
	}
}

func TestEnrichSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "key123" {
			t.Errorf("missing/incorrect auth header: %q", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1337", Category: "T1566", CorrelationID: 42})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "key123", testOpts())
	got, err := c.Enrich(context.Background(), model.Logline{ID: 1, Category: "phishing"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Category != "T1566" || got.ASN != "ASN1337" || got.CorrelationID != 42 {
		t.Fatalf("unexpected details: %+v", got)
	}
}

func TestEnrichRetriesThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{Category: "T1190"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", testOpts())
	got, err := c.Enrich(context.Background(), model.Logline{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Category != "T1190" {
		t.Fatalf("got %+v", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls (2 failures + success), got %d", calls)
	}
}

func TestEnrichBadRequestNotRetried(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", testOpts())
	if _, err := c.Enrich(context.Background(), model.Logline{}); err == nil {
		t.Fatal("expected error on 400")
	}
	if calls != 1 {
		t.Fatalf("400 should not be retried, got %d calls", calls)
	}
}

func TestEnrichCircuitBreakerOpens(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", testOpts()) // BreakerThreshold=3, cooldown 50ms

	for i := 0; i < 3; i++ {
		if _, err := c.Enrich(context.Background(), model.Logline{}); err == nil {
			t.Fatal("expected failure")
		}
	}
	if open, _, _ := c.breaker.allow(); open {
		t.Fatal("breaker should be open after BreakerThreshold failures")
	}
}

func TestEnrichWaitsOutABrownout(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) <= 12 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1337", Category: "T1566", CorrelationID: 7})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", testOpts())
	for i := 0; i < 3; i++ {
		if _, err := c.Enrich(context.Background(), model.Logline{}); err == nil {
			t.Fatal("expected failure while the service is down")
		}
	}

	details, err := c.Enrich(context.Background(), model.Logline{})
	if err != nil {
		t.Fatalf("record dropped instead of waiting out the breaker: %v", err)
	}
	if details.Category != "T1566" {
		t.Fatalf("got %+v", details)
	}
}

func TestEnrichFailsFastWhenHardDown(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", testOpts())

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := c.Enrich(context.Background(), model.Logline{}); err == ErrCircuitOpen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("breaker never gave up on the dead service")
		}
	}

	callsBefore := atomic.LoadInt32(&calls)
	start := time.Now()
	for i := 0; i < 50; i++ {
		if _, err := c.Enrich(context.Background(), model.Logline{}); err != ErrCircuitOpen {
			t.Fatalf("expected ErrCircuitOpen, got %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Fatalf("hard-down breaker still made callers wait: %s for 50 calls", elapsed)
	}
	if atomic.LoadInt32(&calls) != callsBefore {
		t.Fatal("open breaker should not make network calls")
	}
}
