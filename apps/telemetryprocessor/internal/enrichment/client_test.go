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
		Timeout:         2 * time.Second,
		MaxAttempts:     4,
		BackoffBase:     time.Millisecond,
		BackoffMax:      2 * time.Millisecond,
		BreakerTrip:     3,
		BreakerCooldown: 50 * time.Millisecond,
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

	c := NewClient(srv.URL, "k", testOpts()) // BreakerTrip=3

	// Each Enrich exhausts its retries and counts as one breaker failure.
	// After 3 such failures the breaker opens and short-circuits.
	for i := 0; i < 3; i++ {
		if _, err := c.Enrich(context.Background(), model.Logline{}); err == nil {
			t.Fatal("expected failure")
		}
	}
	callsBeforeOpen := atomic.LoadInt32(&calls)

	_, err := c.Enrich(context.Background(), model.Logline{})
	if err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if atomic.LoadInt32(&calls) != callsBeforeOpen {
		t.Fatal("open breaker should not make network calls")
	}

	// After cooldown the breaker half-opens and allows a trial call.
	time.Sleep(60 * time.Millisecond)
	if _, err := c.Enrich(context.Background(), model.Logline{}); err == ErrCircuitOpen {
		t.Fatal("breaker should allow a trial call after cooldown")
	}
	if atomic.LoadInt32(&calls) <= callsBeforeOpen {
		t.Fatal("half-open breaker should make a network call")
	}
}
