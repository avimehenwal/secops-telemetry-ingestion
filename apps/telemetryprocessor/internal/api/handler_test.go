package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

func newHandler(t *testing.T, enrichURL, analyticsURL string) *IngestHandler {
	t.Helper()

	client := analytics.NewClient(analyticsURL, "k", analytics.Options{
		Timeout: time.Second, RateRequests: 1000, RateWindow: time.Second, MaxRetries: 2,
	})
	batcher := analytics.NewBatcher(client.SendBatch, analytics.BatcherOptions{
		BatchSize: 20, MaxDelay: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go batcher.Run(ctx)
	t.Cleanup(func() {
		cancel()
		batcher.Wait()
	})

	return &IngestHandler{
		Enrichment: enrichment.NewClient(enrichURL, "k", enrichment.Options{
			Timeout: time.Second, MaxAttempts: 2, BackoffBase: time.Millisecond,
			BackoffMax: time.Millisecond, BreakerTrip: 100, BreakerCooldown: time.Second,
		}),
		Analytics: batcher,
	}
}

func post(t *testing.T, h http.Handler, req model.IngestRequest) (*httptest.ResponseRecorder, model.IngestResult) {
	t.Helper()
	body, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var res model.IngestResult
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	return w, res
}

func TestHandlerHappyPath(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in model.Logline
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1", Category: "T1566", CorrelationID: in.ID})
	}))
	defer enrichSrv.Close()

	var ingested int
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		ingested += len(batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	w, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "phising"}, // messy spelling -> normalised
		{ID: 2, Asset: "b", IP: "5.6.7.8", Category: "valid accounts"},
	}})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if res.Received != 2 || res.Enriched != 2 || res.Ingested != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if ingested != 2 {
		t.Fatalf("analytics saw %d events, want 2", ingested)
	}
}

func TestHandlerUnknownCategorySkipped(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{Category: "T1566"})
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{ItemsIngested: int64(len(batch))})
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	_, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "totally-unknown"},
		{ID: 2, Asset: "b", IP: "5.6.7.8", Category: "phishing"},
	}})

	if res.FailedCategory != 1 || res.Enriched != 1 || res.Ingested != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(res.RecordErrors) != 1 || res.RecordErrors[0].Stage != "category" {
		t.Fatalf("expected one category error, got %+v", res.RecordErrors)
	}
}

func TestHandlerEnrichmentFailureReported(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("analytics should not be called when enrichment fails")
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	w, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "phishing"},
	}})

	// Nothing enriched and nothing ingested => 502 so the CLI exits non-zero.
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", w.Code)
	}
	if res.FailedEnrichment != 1 || res.Ingested != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestHandlerDryRunSkipsMicroservices(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("enrichment must not be called in dry-run mode")
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("analytics must not be called in dry-run mode")
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	body, _ := json.Marshal(model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "phising"},       // valid -> would ingest
		{ID: 2, Asset: "b", IP: "5.6.7.8", Category: "totally-bogus"}, // bad category
	}})
	r := httptest.NewRequest(http.MethodPost, "/ingest?dry-run=true", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	var res model.IngestResult
	_ = json.Unmarshal(w.Body.Bytes(), &res)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !res.DryRun {
		t.Fatalf("expected DryRun=true in result: %+v", res)
	}
	if res.Received != 2 || res.Ingested != 1 || res.FailedCategory != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.Enriched != 0 {
		t.Fatalf("nothing should be enriched in dry-run, got %d", res.Enriched)
	}
}

func TestHandlerRejectsNonPost(t *testing.T) {
	h := newHandler(t, "http://unused", "http://unused")
	r := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

func TestHandlerRejectsBadJSON(t *testing.T) {
	h := newHandler(t, "http://unused", "http://unused")
	r := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader([]byte("{not json")))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
