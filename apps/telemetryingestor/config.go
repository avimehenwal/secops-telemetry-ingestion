package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	envEndpoint  = "EYESECURITY_ENDPOINT"
	envChunkSize = "EYESECURITY_CHUNK_SIZE"
	envTimeout   = "EYESECURITY_TIMEOUT"

	defaultEndpoint = "http://localhost:8080/ingest"

	// The Analytics Service accepts at most 20 items per request and only one
	// request per 10s window (docs/openapi.json). One chunk therefore maps to
	// exactly one upstream request, which keeps a chunk's latency predictable
	// and bounds how much work a failed chunk puts at risk.
	upstreamBatchSize  = 20
	upstreamRateWindow = 10 * time.Second
	defaultChunkSize   = upstreamBatchSize

	// Slack covers enrichment (one call per record) plus connection overhead
	// on top of the rate-limit floor.
	timeoutSlack = 30 * time.Second
)

func estimateTimeout(chunkSize int) time.Duration {
	requests := (chunkSize + upstreamBatchSize - 1) / upstreamBatchSize
	if requests < 1 {
		requests = 1
	}
	// The first request goes out immediately; only the rest wait for a window.
	return time.Duration(requests-1)*upstreamRateWindow + timeoutSlack
}

func resolveTimeout(flagVal time.Duration, flagSet bool, chunkSize int) (time.Duration, error) {
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
	return estimateTimeout(chunkSize), nil
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

func resolveChunkSize(flagVal int, flagSet bool) (int, error) {
	if flagSet {
		if flagVal <= 0 {
			return 0, fmt.Errorf("-chunk-size must be a positive integer, got %d", flagVal)
		}
		return flagVal, nil
	}
	if v := os.Getenv(envChunkSize); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("%s must be a positive integer, got %q", envChunkSize, v)
		}
		return n, nil
	}
	return flagVal, nil
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
