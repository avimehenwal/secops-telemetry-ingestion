// Package analytics is a client for the Analytics Service
// (docs/openapi.json, POST /analytics).
package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/ratelimit"
)

// UpstreamMaxBatchSize is the hard maximum number of events the Analytics
const UpstreamMaxBatchSize = 20

// Options configures a Client.
type Options struct {
	Timeout    time.Duration // per-attempt HTTP timeout
	BatchSize  int           // max events per request (clamped to UpstreamMaxBatchSize)
	RateLimit  int           // messages permitted per RateWindow
	RateWindow time.Duration // the rate-limit window
	MaxRetries int           // retries on HTTP 429
}

// Client sends enriched events to the Analytics Service.
type Client struct {
	baseURL   string
	apiKey    string
	http      *http.Client
	batchSize int
	maxRetry  int
	limiter   *ratelimit.Bucket
}

// NewClient constructs an analytics Client with a shared rate limiter.
func NewClient(baseURL, apiKey string, opts Options) *Client {
	batch := opts.BatchSize
	if batch < 1 || batch > UpstreamMaxBatchSize {
		batch = UpstreamMaxBatchSize
	}
	return &Client{
		baseURL:   baseURL,
		apiKey:    apiKey,
		http:      &http.Client{Timeout: opts.Timeout},
		batchSize: batch,
		maxRetry:  opts.MaxRetries,
		limiter:   ratelimit.New(opts.RateLimit, opts.RateWindow),
	}
}

func (c *Client) Send(ctx context.Context, events []model.AnalyticsEvent) (int, error) {
	ingested := 0
	for start := 0; start < len(events); start += c.batchSize {
		end := start + c.batchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[start:end]

		if err := c.limiter.Wait(ctx, len(batch)); err != nil {
			return ingested, err
		}

		n, err := c.sendBatch(ctx, batch)
		ingested += n
		if err != nil {
			return ingested, err
		}
	}
	return ingested, nil
}

func (c *Client) sendBatch(ctx context.Context, batch []model.AnalyticsEvent) (int, error) {
	body, err := json.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshalling analytics batch: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetry; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		n, retryAfter, err := c.postOnce(ctx, body, len(batch))
		if err == nil {
			return n, nil
		}
		lastErr = err
		if retryAfter < 0 {
			return 0, err
		}
		if attempt < c.maxRetry {
			if err := sleep(ctx, retryAfter); err != nil {
				return 0, err
			}
		}
	}
	return 0, fmt.Errorf("analytics batch failed after %d attempt(s): %w", c.maxRetry+1, lastErr)
}

func (c *Client) postOnce(ctx context.Context, body []byte, count int) (n int, retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return 0, -1, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, err // network error: retry immediately (limiter still gates rate)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var out model.AnalyticsSuccessResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			// The batch was accepted (200) but we could not parse the count;
			// assume all items ingested rather than double-sending.
			return count, -1, nil
		}
		return int(out.ItemsIngested), 0, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return 0, retryAfterDelay(resp), fmt.Errorf("analytics rate limited (429)")
	case resp.StatusCode >= 500:
		return 0, 0, fmt.Errorf("analytics %s", status(resp))
	default:
		return 0, -1, fmt.Errorf("analytics %s", status(resp))
	}
}

func retryAfterDelay(resp *http.Response) time.Duration {
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return 10 * time.Second
}

func status(resp *http.Response) string {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if len(bytes.TrimSpace(snippet)) == 0 {
		return resp.Status
	}
	return fmt.Sprintf("%s: %s", resp.Status, bytes.TrimSpace(snippet))
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
