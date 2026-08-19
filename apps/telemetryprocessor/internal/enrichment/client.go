package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

type Options struct {
	Timeout         time.Duration // per-attempt HTTP timeout
	MaxAttempts     int           // total attempts including the first
	BackoffBase     time.Duration // first retry delay, doubled each attempt
	BackoffMax      time.Duration // cap on a single backoff sleep
	BreakerTrip     int           // consecutive failures before the breaker opens
	BreakerCooldown time.Duration // how long the breaker stays open
}

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	opts    Options
	breaker *breaker
}

func NewClient(baseURL, apiKey string, opts Options) *Client {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: opts.Timeout},
		opts:    opts,
		breaker: newBreaker(opts.BreakerTrip, opts.BreakerCooldown),
	}
}

var ErrCircuitOpen = errors.New("enrichment circuit breaker is open")

func (c *Client) Enrich(ctx context.Context, rec model.Logline) (model.EnrichmentDetails, error) {
	if !c.breaker.allow() {
		return model.EnrichmentDetails{}, ErrCircuitOpen
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

		details, retryable, err := c.doOnce(ctx, body)
		if err == nil {
			c.breaker.success()
			return details, nil
		}
		lastErr = err
		if !retryable {
			return model.EnrichmentDetails{}, err
		}

		if attempt < c.opts.MaxAttempts {
			if err := sleep(ctx, c.backoff(attempt)); err != nil {
				return model.EnrichmentDetails{}, err
			}
		}
	}

	c.breaker.failure()
	return model.EnrichmentDetails{}, fmt.Errorf("enrichment failed after %d attempts: %w", c.opts.MaxAttempts, lastErr)
}

func (c *Client) doOnce(ctx context.Context, body []byte) (model.EnrichmentDetails, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return model.EnrichmentDetails{}, false, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		// Network/timeout errors are transient.
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
		// Invalid input: permanent, do not retry.
		return model.EnrichmentDetails{}, false, fmt.Errorf("enrichment rejected record: %s", status(resp))
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		// Overload / server error: transient.
		return model.EnrichmentDetails{}, true, fmt.Errorf("enrichment %s", status(resp))
	default:
		return model.EnrichmentDetails{}, false, fmt.Errorf("enrichment unexpected %s", status(resp))
	}
}

func (c *Client) backoff(attempt int) time.Duration {
	d := time.Duration(float64(c.opts.BackoffBase) * math.Pow(2, float64(attempt-1)))
	if c.opts.BackoffMax > 0 && d > c.opts.BackoffMax {
		d = c.opts.BackoffMax
	}
	return d
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
