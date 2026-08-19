package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/category"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

const maxRecordErrors = 100

type pendingRecord struct {
	id     int64
	result <-chan error
}

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
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	dryRun := isDryRun(r)

	ctx := r.Context()
	result := model.IngestResult{Received: len(req.Records), DryRun: dryRun}

	pending := make([]pendingRecord, 0, len(req.Records))
	for _, rec := range req.Records {
		cat, ok := category.Normalize(rec.Category)
		if !ok {
			result.FailedCategory++
			h.addErr(&result, rec.ID, "category", "unrecognised category "+quote(rec.Category))
			continue
		}

		if dryRun {
			result.Ingested++
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
				break
			}
			result.FailedEnrichment++
			h.addErr(&result, rec.ID, "enrichment", err.Error())
			continue
		}
		result.Enriched++

		pending = append(pending, pendingRecord{
			id: rec.ID,
			result: h.Analytics.Submit(ctx, model.AnalyticsEvent{
				ID:            rec.ID,
				Asset:         rec.Asset,
				IP:            rec.IP,
				Category:      details.Category,
				ASN:           details.ASN,
				CorrelationID: details.CorrelationID,
			}),
		})
	}

	for _, p := range pending {
		if err := <-p.result; err != nil {
			result.FailedAnalytics++
			h.addErr(&result, p.id, "analytics", err.Error())
			continue
		}
		result.Ingested++
	}
	if result.FailedAnalytics > 0 {
		h.logf("analytics delivery failed for %d/%d records", result.FailedAnalytics, len(pending))
	}

	h.logf("ingest complete: dryRun=%t received=%d enriched=%d ingested=%d failedCategory=%d failedEnrichment=%d failedAnalytics=%d",
		result.DryRun, result.Received, result.Enriched, result.Ingested, result.FailedCategory, result.FailedEnrichment, result.FailedAnalytics)

	status := http.StatusOK
	if !dryRun && result.Received > 0 && result.Ingested == 0 && result.Enriched == 0 {
		status = http.StatusBadGateway
	}
	writeJSON(w, status, result)
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
