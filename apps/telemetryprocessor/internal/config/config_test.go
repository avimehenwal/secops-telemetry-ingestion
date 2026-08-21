package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// No env vars set (t.Setenv not used) -> defaults.
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" {
		t.Errorf("Addr = %q", c.Addr)
	}
	if c.APIKey != "eye-am-hiring" {
		t.Errorf("APIKey = %q", c.APIKey)
	}
	if c.AnalyticsRateRequests != 1 || c.AnalyticsRateWindow != 10*time.Second {
		t.Errorf("rate = %d req/%s, want 1 req/10s per the OpenAPI spec", c.AnalyticsRateRequests, c.AnalyticsRateWindow)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("PROCESSOR_ADDR", ":9999")
	t.Setenv("EYE_API_KEY", "secret")
	t.Setenv("ANALYTICS_RATE_REQUESTS", "5")
	t.Setenv("ANALYTICS_RATE_WINDOW", "3s")
	t.Setenv("ENRICHMENT_BACKOFF_BASE", "50ms")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":9999" || c.APIKey != "secret" {
		t.Errorf("overrides not applied: %+v", c)
	}
	if c.AnalyticsRateRequests != 5 || c.AnalyticsRateWindow != 3*time.Second {
		t.Errorf("rate override not applied: %d/%s", c.AnalyticsRateRequests, c.AnalyticsRateWindow)
	}
	if c.EnrichmentBackoffBase != 50*time.Millisecond {
		t.Errorf("backoff override not applied: %s", c.EnrichmentBackoffBase)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	t.Setenv("ANALYTICS_BATCH_SIZE", "999") // > upstream max
	if _, err := Load(); err == nil {
		t.Fatal("expected validation error for oversized batch")
	}
}

func TestLoadRejectsMalformedDuration(t *testing.T) {
	t.Setenv("ENRICHMENT_TIMEOUT", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected parse error for bad duration")
	}
}
