package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr          string
	EnrichmentURL string
	AnalyticsURL  string
	APIKey        string

	EnrichmentTimeout         time.Duration // per-attempt HTTP timeout
	EnrichmentMaxAttempts     int           // total attempts (1 == no retry)
	EnrichmentBackoffBase     time.Duration // base delay, doubled each retry
	EnrichmentBackoffMax      time.Duration // cap on a single backoff sleep
	EnrichmentBreakerTrip     int           // consecutive failures before the breaker opens
	EnrichmentBreakerCooldown time.Duration // how long the breaker stays open

	AnalyticsTimeout     time.Duration // per-attempt HTTP timeout
	AnalyticsBatchSize   int           // max events per request (upstream max is 20)
	AnalyticsBatchDelay  time.Duration // how long a partial batch waits for company
	AnalyticsQueueSize   int           // events buffered before ingest requests block
	AnalyticsBatchGrace  time.Duration // budget for delivering queued events at shutdown
	AnalyticsRateRequest int           // requests permitted per RateWindow (upstream allows 1)
	AnalyticsRateWindow  time.Duration // the rate-limit window
	AnalyticsMaxRetries  int           // retries on HTTP 429 before giving up
}

func Load() (Config, error) {
	c := Config{
		Addr:          getenv("PROCESSOR_ADDR", ":8080"),
		EnrichmentURL: getenv("ENRICHMENT_URL", "https://api.heyering.com/enrichment"),
		AnalyticsURL:  getenv("ANALYTICS_URL", "https://api.heyering.com/analytics"),
		APIKey:        getenv("EYE_API_KEY", "eye-am-hiring"),
	}

	var err error
	if c.EnrichmentTimeout, err = getenvDuration("ENRICHMENT_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if c.EnrichmentMaxAttempts, err = getenvInt("ENRICHMENT_MAX_ATTEMPTS", 4); err != nil {
		return Config{}, err
	}
	if c.EnrichmentBackoffBase, err = getenvDuration("ENRICHMENT_BACKOFF_BASE", 200*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.EnrichmentBackoffMax, err = getenvDuration("ENRICHMENT_BACKOFF_MAX", 5*time.Second); err != nil {
		return Config{}, err
	}
	if c.EnrichmentBreakerTrip, err = getenvInt("ENRICHMENT_BREAKER_TRIP", 5); err != nil {
		return Config{}, err
	}
	if c.EnrichmentBreakerCooldown, err = getenvDuration("ENRICHMENT_BREAKER_COOLDOWN", 15*time.Second); err != nil {
		return Config{}, err
	}

	if c.AnalyticsTimeout, err = getenvDuration("ANALYTICS_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if c.AnalyticsBatchSize, err = getenvInt("ANALYTICS_BATCH_SIZE", 20); err != nil {
		return Config{}, err
	}
	if c.AnalyticsBatchDelay, err = getenvDuration("ANALYTICS_BATCH_DELAY", 2*time.Second); err != nil {
		return Config{}, err
	}
	if c.AnalyticsQueueSize, err = getenvInt("ANALYTICS_QUEUE_SIZE", 200); err != nil {
		return Config{}, err
	}
	if c.AnalyticsBatchGrace, err = getenvDuration("ANALYTICS_BATCH_GRACE", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.AnalyticsRateRequest, err = getenvInt("ANALYTICS_RATE_REQUESTS", 1); err != nil {
		return Config{}, err
	}
	if c.AnalyticsRateWindow, err = getenvDuration("ANALYTICS_RATE_WINDOW", 10*time.Second); err != nil {
		return Config{}, err
	}
	if c.AnalyticsMaxRetries, err = getenvInt("ANALYTICS_MAX_RETRIES", 5); err != nil {
		return Config{}, err
	}

	return c, c.validate()
}

func (c Config) validate() error {
	switch {
	case c.EnrichmentMaxAttempts < 1:
		return fmt.Errorf("ENRICHMENT_MAX_ATTEMPTS must be >= 1, got %d", c.EnrichmentMaxAttempts)
	case c.EnrichmentBreakerTrip < 1:
		return fmt.Errorf("ENRICHMENT_BREAKER_TRIP must be >= 1, got %d", c.EnrichmentBreakerTrip)
	case c.AnalyticsBatchSize < 1 || c.AnalyticsBatchSize > 20:
		return fmt.Errorf("ANALYTICS_BATCH_SIZE must be between 1 and 20, got %d", c.AnalyticsBatchSize)
	case c.AnalyticsBatchDelay <= 0:
		return fmt.Errorf("ANALYTICS_BATCH_DELAY must be > 0, got %s", c.AnalyticsBatchDelay)
	case c.AnalyticsQueueSize < 1:
		return fmt.Errorf("ANALYTICS_QUEUE_SIZE must be >= 1, got %d", c.AnalyticsQueueSize)
	case c.AnalyticsRateRequest < 1:
		return fmt.Errorf("ANALYTICS_RATE_REQUESTS must be >= 1, got %d", c.AnalyticsRateRequest)
	case c.AnalyticsRateWindow <= 0:
		return fmt.Errorf("ANALYTICS_RATE_WINDOW must be > 0, got %s", c.AnalyticsRateWindow)
	// A negative retry budget would skip the send loop entirely and report a
	// failure with no underlying cause, which is a confusing way to find out
	// about a typo in an env var.
	case c.AnalyticsMaxRetries < 0:
		return fmt.Errorf("ANALYTICS_MAX_RETRIES must be >= 0, got %d", c.AnalyticsMaxRetries)
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", key, v)
	}
	return n, nil
}

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration (e.g. 200ms, 10s), got %q", key, v)
	}
	return d, nil
}
