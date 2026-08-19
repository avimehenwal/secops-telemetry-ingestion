// Package enrichment is a client for the Enrichment Service
// (docs/openapi.json, POST /enrichment).
//
// The service has known intermittent reliability issues (per the Jira
// ticket, it's unclear whether the Canary Team has fixed them), so the real
// implementation will need retries with backoff and a circuit breaker
// around this client rather than calling it naively per record.
package enrichment

import (
	"context"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

// Client enriches individual records via the Enrichment Service.
type Client struct {
	baseURL string
	apiKey  string
}

// NewClient constructs an enrichment Client.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey}
}

// Enrich calls the Enrichment Service for a single record.
//
// TODO: implement the HTTP call, and wrap it with retry/backoff + a circuit
// breaker to protect the processor from the service's known intermittent
// failures.
func (c *Client) Enrich(ctx context.Context, record model.Record) (model.EnrichmentDetails, error) {
	panic("not implemented")
}
