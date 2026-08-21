package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
	"github.com/avimehenwal/secops-telemetry-ingestion/libs/category"
)

const (
	maxReportedErrors = 100
	progressInterval  = 20
	outcomeBuffer     = progressInterval
	ndjsonContentType = "application/x-ndjson"
)

type IngestHandler struct {
	Enrichment *enrichment.Client
	Analytics  *analytics.Batcher
	Logger     *log.Logger
}

func (h *IngestHandler) logf(format string, args ...any) {
	if h.Logger != nil {
		h.Logger.Printf(format, args...)
	}
}

func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.logf("request received: method=%s path=%s remoteAddr=%s", r.Method, r.URL.Path, r.RemoteAddr)

	if r.Method != http.MethodPost {
		h.logf("response sent: status=%d reason=method-not-allowed", http.StatusMethodNotAllowed)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logf("response sent: status=%d reason=invalid-body error=%q", http.StatusBadRequest, err.Error())
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	dryRun := isDryRun(r)
	h.logf("request body decoded: records=%d dryRun=%t", len(req.Records), dryRun)

	// Streaming is opt-in and needs a ResponseWriter that can flush, so the
	// plain path stays the default and the fallback.
	if flusher, ok := w.(http.Flusher); ok && wantsStream(r) {
		h.serveStream(w, flusher, r, req, dryRun)
		return
	}

	result := h.runPipeline(r.Context(), req.Records, dryRun, nil)
	status := statusFor(result)
	h.logResult(result)
	h.logf("response sent: status=%d", status)
	writeJSON(w, status, result)
}

func (h *IngestHandler) serveStream(w http.ResponseWriter, flusher http.Flusher, r *http.Request, req model.IngestRequest, dryRun bool) {
	w.Header().Set("Content-Type", ndjsonContentType)
	w.Header().Set("Cache-Control", "no-store")
	// Chunked encoding without this can still be coalesced by an intermediary.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w)
	writeFrame := func(frame model.IngestResult) {
		// A failed write means the client hung up. The work continues -- the
		// batcher owns it now -- but there is nobody left to narrate it to.
		if err := enc.Encode(frame); err != nil {
			return
		}
		flusher.Flush()
	}

	result := h.runPipeline(r.Context(), req.Records, dryRun, writeFrame)
	result.Kind = model.KindResult
	writeFrame(result)
	h.logResult(result)
	h.logf("response sent: status=%d streamed=true", http.StatusOK)
}

type recordOutcome struct {
	id       int64
	stage    model.Stage // set only on failure; empty when delivery is set
	reason   string
	delivery <-chan error
}

func (h *IngestHandler) runPipeline(ctx context.Context, records []model.Record, dryRun bool, onProgress func(model.IngestResult)) model.IngestResult {
	result := model.IngestResult{Received: len(records), DryRun: dryRun}

	if dryRun {
		return h.dryRun(result, records)
	}

	var enrichedAhead atomic.Int64

	outcomes := make(chan recordOutcome, outcomeBuffer)
	go h.produce(ctx, records, outcomes, &enrichedAhead)

	settled := 0
	for outcome := range outcomes {
		if outcome.delivery == nil {
			h.countFailure(&result, outcome.id, outcome.stage, outcome.reason)
		} else {
			result.Enriched++
			if err := <-outcome.delivery; err != nil {
				h.countFailure(&result, outcome.id, model.StageAnalytics, err.Error())
			} else {
				result.Ingested++
			}
		}

		settled++
		if settled%progressInterval == 0 {
			h.logf("ingest progress: settled=%d/%d enriched=%d ingested=%d failed=%d",
				settled, len(records), enrichedAhead.Load(), result.Ingested,
				result.FailedCategory+result.FailedEnrichment+result.FailedAnalytics)
			if onProgress != nil {
				onProgress(progressFrame(result, enrichedAhead.Load()))
			}
		}
	}

	if result.FailedAnalytics > 0 {
		h.logf("analytics delivery failed for %d record(s)", result.FailedAnalytics)
	}
	return result
}

func (h *IngestHandler) produce(ctx context.Context, records []model.Record, outcomes chan<- recordOutcome, enrichedAhead *atomic.Int64) {
	defer close(outcomes)

	for _, rec := range records {
		normalised, ok := category.Normalize(rec.Category)
		if !ok {
			outcomes <- recordOutcome{
				id:     rec.ID,
				stage:  model.StageCategory,
				reason: fmt.Sprintf("unrecognised category %q", rec.Category),
			}
			continue
		}

		details, err := h.Enrichment.Enrich(ctx, rec.Logline(normalised))
		if err != nil {
			if ctx.Err() != nil {
				h.logf("ingest aborted mid-enrichment: %v", ctx.Err())
				return
			}
			outcomes <- recordOutcome{id: rec.ID, stage: model.StageEnrichment, reason: err.Error()}
			continue
		}
		enrichedAhead.Add(1)

		outcomes <- recordOutcome{
			id:       rec.ID,
			delivery: h.Analytics.Submit(ctx, rec.AnalyticsEvent(details)),
		}
	}
}

func (h *IngestHandler) dryRun(result model.IngestResult, records []model.Record) model.IngestResult {
	for _, rec := range records {
		if _, ok := category.Normalize(rec.Category); !ok {
			h.countFailure(&result, rec.ID, model.StageCategory,
				fmt.Sprintf("unrecognised category %q", rec.Category))
			continue
		}
		result.Ingested++
	}
	return result
}

func progressFrame(result model.IngestResult, enrichedAhead int64) model.IngestResult {
	result.Kind = model.KindProgress
	result.RecordErrors = nil
	result.Enriched = int(enrichedAhead)
	return result
}

func (h *IngestHandler) countFailure(result *model.IngestResult, id int64, stage model.Stage, reason string) {
	switch stage {
	case model.StageCategory:
		result.FailedCategory++
	case model.StageEnrichment:
		result.FailedEnrichment++
	case model.StageAnalytics:
		result.FailedAnalytics++
	}
	if len(result.RecordErrors) < maxReportedErrors {
		result.RecordErrors = append(result.RecordErrors,
			model.RecordError{ID: id, Stage: stage, Reason: reason})
	}
}

func (h *IngestHandler) logResult(result model.IngestResult) {
	h.logf("ingest complete: dryRun=%t received=%d enriched=%d ingested=%d failedCategory=%d failedEnrichment=%d failedAnalytics=%d",
		result.DryRun, result.Received, result.Enriched, result.Ingested,
		result.FailedCategory, result.FailedEnrichment, result.FailedAnalytics)
}

func statusFor(result model.IngestResult) int {
	if result.DryRun {
		return http.StatusOK
	}
	if attempted := result.Received - result.FailedCategory; attempted > 0 && result.Ingested == 0 {
		return http.StatusBadGateway
	}
	return http.StatusOK
}

func wantsStream(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), ndjsonContentType)
}

func isDryRun(r *http.Request) bool {
	switch strings.ToLower(r.URL.Query().Get("dry-run")) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
