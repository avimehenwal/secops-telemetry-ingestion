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
		BatchSize: 20, MaxFillWait: 10 * time.Millisecond,
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
			BackoffMax: time.Millisecond, BreakerThreshold: 100, BreakerCooldown: time.Second,
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
	if len(res.RecordErrors) != 1 || res.RecordErrors[0].Stage != model.StageCategory {
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

// A payload whose records are all unrecognised categories is a bad CSV, not a
// broken dependency. 502 would point the operator at the Enrichment Service.
func TestHandlerAllBadCategoriesIsNotBadGateway(t *testing.T) {
	unreachable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no upstream should be called when every category is unrecognised")
	}))
	defer unreachable.Close()

	h := newHandler(t, unreachable.URL, unreachable.URL)
	w, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "cryptomining"},
		{ID: 2, Asset: "b", IP: "5.6.7.8", Category: "gibberish"},
	}})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the data is bad, the pipeline is fine)", w.Code)
	}
	if res.FailedCategory != 2 || res.Ingested != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

// The opposite case still has to fail loudly: records reached Analytics and
// none of them stuck.
func TestHandlerAnalyticsTotalFailureIsBadGateway(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in model.Logline
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1", Category: "T1566", CorrelationID: in.ID})
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	w, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "phishing"},
	}})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", w.Code)
	}
	if res.Enriched != 1 || res.Ingested != 0 || res.FailedAnalytics != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestHandlerStreamsProgress(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in model.Logline
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1", Category: "T1566", CorrelationID: in.ID})
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer analyticsSrv.Close()

	// 45 records => progress at 20 and 40, then the result.
	records := make([]model.Record, 45)
	for i := range records {
		records[i] = model.Record{ID: int64(i + 1), Asset: "a", IP: "1.2.3.4", Category: "phishing"}
	}

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	body, _ := json.Marshal(model.IngestRequest{Records: records})
	r := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	r.Header.Set("Accept", ndjsonContentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if ct := w.Header().Get("Content-Type"); ct != ndjsonContentType {
		t.Fatalf("Content-Type = %q, want %q", ct, ndjsonContentType)
	}

	var progress []model.IngestResult
	var final *model.IngestResult
	dec := json.NewDecoder(w.Body)
	for {
		var ev model.IngestResult
		if err := dec.Decode(&ev); err != nil {
			break
		}
		switch ev.Kind {
		case model.KindProgress:
			progress = append(progress, ev)
		case model.KindResult:
			ev := ev
			final = &ev
		}
	}

	if len(progress) != 2 {
		t.Fatalf("got %d progress events, want 2", len(progress))
	}
	// Progress must actually progress, and never overstate the final tally.
	if progress[0].Ingested != 20 || progress[1].Ingested != 40 {
		t.Fatalf("progress ingested counts = %d, %d; want 20, 40",
			progress[0].Ingested, progress[1].Ingested)
	}
	if final == nil {
		t.Fatal("stream ended without a result event")
	}
	if final.Ingested != 45 || final.Received != 45 {
		t.Fatalf("final result = %+v, want 45 received and ingested", *final)
	}
}

// Without the Accept header the response must be exactly what it always was.
func TestHandlerDoesNotStreamWithoutAcceptHeader(t *testing.T) {
	enrichSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in model.Logline
		_ = json.NewDecoder(r.Body).Decode(&in)
		_ = json.NewEncoder(w).Encode(model.EnrichmentDetails{ASN: "ASN1", Category: "T1566", CorrelationID: in.ID})
	}))
	defer enrichSrv.Close()
	analyticsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []model.AnalyticsEvent
		_ = json.NewDecoder(r.Body).Decode(&batch)
		_ = json.NewEncoder(w).Encode(model.AnalyticsSuccessResponse{Status: "ok", ItemsIngested: int64(len(batch))})
	}))
	defer analyticsSrv.Close()

	h := newHandler(t, enrichSrv.URL, analyticsSrv.URL)
	w, res := post(t, h, model.IngestRequest{Records: []model.Record{
		{ID: 1, Asset: "a", IP: "1.2.3.4", Category: "phishing"},
	}})

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if res.Kind != "" {
		t.Fatalf("non-streaming response leaked a kind discriminator: %q", res.Kind)
	}
	if res.Ingested != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}
