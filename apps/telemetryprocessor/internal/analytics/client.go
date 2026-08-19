// Package analytics is a client for the Analytics Service
// (docs/openapi.json, POST /analytics).
//
// The service is rate limited to 20 messages per 10 seconds following a
// prior budget incident, and accepts batches of up to 20 events per
// request. The real implementation will need a rate limiter (e.g. a token
// bucket) shared across requests, plus batching of outgoing events.
package analytics

import (
	"context"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

const (
	// MaxBatchSize is the maximum number of events the Analytics Service
	// accepts per request.
	MaxBatchSize = 20
	// RateLimitPerWindow is the maximum number of requests allowed per
	// RateLimitWindowSeconds.
	RateLimitPerWindow = 20
	// RateLimitWindowSeconds is the window over which RateLimitPerWindow
	// applies.
	RateLimitWindowSeconds = 10
)

// Client sends enriched events to the Analytics Service.
type Client struct {
	baseURL string
	apiKey  string
}

// NewClient constructs an analytics Client.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey}
}

// Send delivers a batch of events (at most MaxBatchSize) to the Analytics
// Service.
//
// TODO: implement the HTTP call, batching of the caller's events into
// MaxBatchSize chunks, and rate limiting to RateLimitPerWindow requests per
// RateLimitWindowSeconds.
func (c *Client) Send(ctx context.Context, events []model.AnalyticsEvent) error {
	panic("not implemented")
}
