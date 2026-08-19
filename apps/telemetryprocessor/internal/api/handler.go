// Package api implements the HTTP handler for the processor's single
// ingest endpoint.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

// IngestHandler handles POST requests from the CLI containing records to
// enrich and forward to the Analytics Service.
type IngestHandler struct {
	Enrichment *enrichment.Client
	Analytics  *analytics.Client
}

func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// TODO: for each record, call h.Enrichment.Enrich, merge the result
	// into a model.AnalyticsEvent, and forward batches to h.Analytics.Send.
	// TODO: decide on partial-failure semantics (e.g. per-record status in
	// the response) once enrichment/analytics failure modes are handled.

	w.WriteHeader(http.StatusAccepted)
}
