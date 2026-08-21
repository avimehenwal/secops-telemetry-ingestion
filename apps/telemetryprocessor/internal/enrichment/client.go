package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

type Options struct {
	Timeout          time.Duration // per-attempt HTTP timeout
	MaxAttempts      int           // total attempts including the first
	BackoffBase      time.Duration // first retry delay, doubled each attempt
	BackoffMax       time.Duration // cap on a single backoff sleep
	BreakerThreshold int           // consecutive failures before the breaker opens
	BreakerCooldown  time.Duration // how long the breaker stays open
	Logger           *slog.Logger  // optional; retries and breaker waits are logged here
}

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	opts    Options
	breaker *breaker
	logger  *slog.Logger
}

func NewClient(baseURL, apiKey string, opts Options) *Client {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: opts.Timeout},
		opts:    opts,
		breaker: newBreaker(opts.BreakerThreshold, opts.BreakerCooldown),
		logger:  logger,
	}
}

var ErrCircuitOpen = errors.New("enrichment circuit breaker is open")

func (c *Client) Enrich(ctx context.Context, rec model.Logline) (model.EnrichmentDetails, error) {
	for {
		ok, retryIn, hardDown := c.breaker.allow()
		if ok {
			break
		}
		if hardDown {
			return model.EnrichmentDetails{}, ErrCircuitOpen
		}
		c.logger.Warn("circuit breaker open, waiting before next attempt", "wait", retryIn.String())
		if err := sleep(ctx, retryIn); err != nil {
			return model.EnrichmentDetails{}, err
		}
	}

	body, err := json.Marshal(rec)
	if err != nil {
		return model.EnrichmentDetails{}, fmt.Errorf("marshalling logline: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= c.opts.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return model.EnrichmentDetails{}, err
		}

		details, retryable, err := c.enrichOnce(ctx, body)
		if err == nil {
			c.breaker.success()
			return details, nil
		}
		lastErr = err
		if !retryable {
			return model.EnrichmentDetails{}, err
		}

		if attempt < c.opts.MaxAttempts {
			backoff := c.backoff(attempt)
			c.logger.Warn("upstream request failed, retrying",
				"attempt", attempt,
				"maxAttempts", c.opts.MaxAttempts,
				"backoff", backoff.String(),
				"error", err.Error())
			if err := sleep(ctx, backoff); err != nil {
				return model.EnrichmentDetails{}, err
			}
		}
	}

	c.breaker.failure()
	return model.EnrichmentDetails{}, fmt.Errorf("enrichment failed after %d attempts: %w", c.opts.MaxAttempts, lastErr)
}

func (c *Client) enrichOnce(ctx context.Context, body []byte) (model.EnrichmentDetails, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return model.EnrichmentDetails{}, false, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return model.EnrichmentDetails{}, true, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var details model.EnrichmentDetails
		if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
			return model.EnrichmentDetails{}, true, fmt.Errorf("decoding response: %w", err)
		}
		return details, false, nil
	case resp.StatusCode == http.StatusBadRequest:
		return model.EnrichmentDetails{}, false, fmt.Errorf("enrichment rejected record: %s", describe(resp))
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		// Overload / server error: transient.
		return model.EnrichmentDetails{}, true, fmt.Errorf("enrichment %s", describe(resp))
	default:
		return model.EnrichmentDetails{}, false, fmt.Errorf("enrichment unexpected %s", describe(resp))
	}
}

func (c *Client) backoff(attempt int) time.Duration {
	d := time.Duration(float64(c.opts.BackoffBase) * math.Pow(2, float64(attempt-1)))
	if c.opts.BackoffMax > 0 && d > c.opts.BackoffMax {
		d = c.opts.BackoffMax
	}
	return d
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
