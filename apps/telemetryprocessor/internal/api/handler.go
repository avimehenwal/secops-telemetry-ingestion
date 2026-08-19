package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
	"github.com/avimehenwal/secops-telemetry-ingestion/libs/category"
)

const maxRecordErrors = 100

const progressEvery = 20

const ndjsonContentType = "application/x-ndjson"

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

	result := h.process(r.Context(), req.Records, dryRun, nil)
	status := statusFor(result, dryRun)
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
	write := func(ev model.IngestResult) {
		// A failed write means the client hung up. The work continues -- the
		// batcher owns it now -- but there is nobody left to narrate it to.
		if err := enc.Encode(ev); err != nil {
			return
		}
		flusher.Flush()
	}

	result := h.process(r.Context(), req.Records, dryRun, write)
	result.Type = "result"
	write(result)
	h.logResult(result)
	h.logf("response sent: status=%d streamed=true", http.StatusOK)
}

// recordEvent is one record's journey as far as the producer could take it:
// either a failure with the stage that caused it, or a pending Analytics
// delivery to be awaited.
type recordEvent struct {
	id     int64
	stage  string // "category" | "enrichment"; empty when result is set
	reason string
	result <-chan error
}

func (h *IngestHandler) process(ctx context.Context, records []model.Record, dryRun bool, onProgress func(model.IngestResult)) model.IngestResult {
	result := model.IngestResult{Received: len(records), DryRun: dryRun}

	if dryRun {
		for _, rec := range records {
			if _, ok := category.Normalize(rec.Category); !ok {
				result.FailedCategory++
				h.addErr(&result, rec.ID, "category", "unrecognised category "+quote(rec.Category))
				continue
			}
			result.Ingested++
		}
		return result
	}

	var enrichedAhead atomic.Int64

	events := make(chan recordEvent, progressEvery)
	go func() {
		defer close(events)
		for _, rec := range records {
			cat, ok := category.Normalize(rec.Category)
			if !ok {
				events <- recordEvent{id: rec.ID, stage: "category",
					reason: "unrecognised category " + quote(rec.Category)}
				continue
			}

			details, err := h.Enrichment.Enrich(ctx, model.Logline{
				ID:       rec.ID,
				Asset:    rec.Asset,
				IP:       rec.IP,
				Category: cat,
			})
			if err != nil {
				if ctx.Err() != nil {
					h.logf("ingest aborted mid-enrichment: %v", ctx.Err())
					return
				}
				events <- recordEvent{id: rec.ID, stage: "enrichment", reason: err.Error()}
				continue
			}
			enrichedAhead.Add(1)

			events <- recordEvent{id: rec.ID, result: h.Analytics.Submit(ctx, model.AnalyticsEvent{
				ID:            rec.ID,
				Asset:         rec.Asset,
				IP:            rec.IP,
				Category:      details.Category,
				ASN:           details.ASN,
				CorrelationID: details.CorrelationID,
			})}
		}
	}()

	settled := 0
	for ev := range events {
		switch {
		case ev.result == nil:
			if ev.stage == "category" {
				result.FailedCategory++
			} else {
				result.FailedEnrichment++
			}
			h.addErr(&result, ev.id, ev.stage, ev.reason)
		default:
			result.Enriched++
			if err := <-ev.result; err != nil {
				result.FailedAnalytics++
				h.addErr(&result, ev.id, "analytics", err.Error())
			} else {
				result.Ingested++
			}
		}

		settled++
		if onProgress != nil && settled%progressEvery == 0 {
			onProgress(progressSnapshot(result, enrichedAhead.Load()))
		}
	}

	if result.FailedAnalytics > 0 {
		h.logf("analytics delivery failed for %d record(s)", result.FailedAnalytics)
	}
	return result
}

// progressSnapshot is a value copy: interim events carry the counts but not the
// per-record error list, which only grows and belongs on the final result.
func progressSnapshot(res model.IngestResult, enrichedAhead int64) model.IngestResult {
	res.Type = "progress"
	res.RecordErrors = nil
	res.Enriched = int(enrichedAhead)
	return res
}

func (h *IngestHandler) logResult(result model.IngestResult) {
	h.logf("ingest complete: dryRun=%t received=%d enriched=%d ingested=%d failedCategory=%d failedEnrichment=%d failedAnalytics=%d",
		result.DryRun, result.Received, result.Enriched, result.Ingested,
		result.FailedCategory, result.FailedEnrichment, result.FailedAnalytics)
}

func statusFor(res model.IngestResult, dryRun bool) int {
	if dryRun {
		return http.StatusOK
	}
	attempted := res.Received - res.FailedCategory
	if attempted > 0 && res.Ingested == 0 {
		return http.StatusBadGateway
	}
	return http.StatusOK
}

func wantsStream(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), ndjsonContentType)
}

func isDryRun(r *http.Request) bool {
	switch r.URL.Query().Get("dry-run") {
	case "1", "true", "TRUE", "True", "yes":
		return true
	default:
		return false
	}
}

func (h *IngestHandler) addErr(res *model.IngestResult, id int64, stage, reason string) {
	if len(res.RecordErrors) >= maxRecordErrors {
		return
	}
	res.RecordErrors = append(res.RecordErrors, model.RecordError{ID: id, Stage: stage, Reason: reason})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func quote(s string) string {
	return `"` + s + `"`
}
