package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/ratelimit"
)

const UpstreamMaxBatchSize = 20

const defaultRetryAfter = 10 * time.Second

type Options struct {
	Timeout      time.Duration // per-attempt HTTP timeout
	RateRequests int           // requests permitted per RateWindow (upstream allows 1)
	RateWindow   time.Duration // the rate-limit window
	MaxRetries   int           // retries on HTTP 429
	Logger       *log.Logger   // optional; retries are logged here
}

type Client struct {
	baseURL    string
	apiKey     string
	http       *http.Client
	maxRetries int
	limiter    *ratelimit.Bucket
	logger     *log.Logger
}

func NewClient(baseURL, apiKey string, opts Options) *Client {
	return &Client{
		baseURL:    baseURL,
		apiKey:     apiKey,
		http:       &http.Client{Timeout: opts.Timeout},
		maxRetries: opts.MaxRetries,
		limiter:    ratelimit.New(opts.RateRequests, opts.RateWindow),
		logger:     opts.Logger,
	}
}

func (c *Client) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

type attempt struct {
	ingested  int
	err       error
	permanent bool          // no retry can change this outcome
	retryIn   time.Duration // how long to wait before retrying
}

func (a attempt) ok() bool { return a.err == nil }

func (c *Client) SendBatch(ctx context.Context, batch []model.AnalyticsEvent) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if len(batch) > UpstreamMaxBatchSize {
		return 0, fmt.Errorf("batch of %d exceeds the upstream maximum of %d", len(batch), UpstreamMaxBatchSize)
	}

	body, err := json.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshalling analytics batch: %w", err)
	}

	var lastErr error
	for try := 0; try <= c.maxRetries; try++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		// The upstream limit is expressed per request, not per item, so every
		// attempt -- including a retry after a 429 -- costs exactly one token.
		if err := c.limiter.Wait(ctx, 1); err != nil {
			return 0, err
		}

		result := c.sendOnce(ctx, body, len(batch))
		if result.ok() {
			return result.ingested, nil
		}
		lastErr = result.err
		if result.permanent {
			return result.ingested, result.err
		}
		if try < c.maxRetries {
			c.logf("analytics retry: attempt=%d/%d error=%v backoff=%s", try+1, c.maxRetries+1, result.err, result.retryIn)
			if err := sleep(ctx, result.retryIn); err != nil {
				return 0, err
			}
		}
	}
	return 0, fmt.Errorf("analytics batch failed after %d attempt(s): %w", c.maxRetries+1, lastErr)
}

func (c *Client) sendOnce(ctx context.Context, body []byte, count int) attempt {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return attempt{err: fmt.Errorf("building request: %w", err), permanent: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		// Network error: retry immediately (the limiter still gates the rate).
		return attempt{err: err}
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var body model.AnalyticsSuccessResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			// A 200 we cannot parse: assume the batch landed. Retrying would
			// duplicate events upstream, which is worse than an optimistic count.
			return attempt{ingested: count}
		}
		if body.Status == "error" {
			// 200 + status=error means the batch was not (fully) stored. Not
			// retryable: a replay would duplicate whatever itemsIngested covers.
			return attempt{
				ingested:  int(body.ItemsIngested),
				permanent: true,
				err: fmt.Errorf("analytics reported status=error (%d of %d items ingested)",
					body.ItemsIngested, count),
			}
		}
		return attempt{ingested: int(body.ItemsIngested)}

	case resp.StatusCode == http.StatusTooManyRequests:
		return attempt{err: fmt.Errorf("analytics rate limited (429)"), retryIn: retryAfter(resp)}

	case resp.StatusCode >= 500:
		return attempt{err: fmt.Errorf("analytics %s", describe(resp))}

	default:
		return attempt{err: fmt.Errorf("analytics %s", describe(resp)), permanent: true}
	}
}

func retryAfter(resp *http.Response) time.Duration {
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultRetryAfter
}

func describe(resp *http.Response) string {
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
