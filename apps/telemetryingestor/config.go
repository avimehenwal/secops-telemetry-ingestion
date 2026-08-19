package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	envEndpoint = "EYESECURITY_ENDPOINT"
	envTimeout  = "EYESECURITY_TIMEOUT"

	defaultEndpoint = "http://localhost:8080/ingest"

	// The Analytics Service accepts at most 20 items per request and only one
	// request per 10s window (docs/openapi.json). That caps the whole pipeline
	// at 20 records / 10s, which is what the timeout below has to allow for.
	upstreamBatchSize  = 20
	upstreamRateWindow = 10 * time.Second

	// Slack covers enrichment (one call per record) plus connection overhead
	// on top of the rate-limit floor.
	timeoutSlack = 30 * time.Second

	estimateRounding = time.Second
)

// estimateDuration is how long the upstream rate limit alone will take for n
// records: the first batch goes out immediately, every later one waits a
// window.
func estimateDuration(records int) time.Duration {
	batches := (records + upstreamBatchSize - 1) / upstreamBatchSize
	if batches < 1 {
		return 0
	}
	return time.Duration(batches-1) * upstreamRateWindow
}

/*
estimateTimeout derives the HTTP timeout from the work being sent. The
processor cannot answer faster than the Analytics rate limit allows, so a fixed
timeout either fails every large file or hides a genuinely stuck processor.
Both are worse than deriving it.
*/
func estimateTimeout(records int) time.Duration {
	return estimateDuration(records) + timeoutSlack
}

func resolveTimeout(flagVal time.Duration, flagSet bool, records int) (time.Duration, error) {
	if flagSet {
		if flagVal <= 0 {
			return 0, fmt.Errorf("-timeout must be positive, got %s", flagVal)
		}
		return flagVal, nil
	}
	if v := os.Getenv(envTimeout); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("%s must be a positive Go duration (e.g. 90s), got %q", envTimeout, v)
		}
		return d, nil
	}
	return estimateTimeout(records), nil
}

func resolveEndpoint(flagVal string, flagSet bool) string {
	if flagSet {
		return flagVal
	}
	if v := os.Getenv(envEndpoint); v != "" {
		return v
	}
	return flagVal // still the default the flag was initialised with
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
