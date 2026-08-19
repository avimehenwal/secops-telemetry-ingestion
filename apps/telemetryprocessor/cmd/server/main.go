package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/api"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	addr := getenv("PROCESSOR_ADDR", ":8080")
	enrichmentURL := getenv("ENRICHMENT_URL", "https://api.heyering.com/enrichment")
	analyticsURL := getenv("ANALYTICS_URL", "https://api.heyering.com/analytics")
	// Per docs/openapi.json, both upstream services authenticate via this
	// static Authorization header value.
	apiKey := getenv("EYE_API_KEY", "eye-am-hiring")

	handler := &api.IngestHandler{
		Enrichment: enrichment.NewClient(enrichmentURL, apiKey),
		Analytics:  analytics.NewClient(analyticsURL, apiKey),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintln(w, "Hello from go world")
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Status    string `json:"status"`
			Timestamp string `json:"timestamp"`
		}{
			Status:    "ok",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.Handle("/ingest", handler)

	log.Printf("telemetryprocessor listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
