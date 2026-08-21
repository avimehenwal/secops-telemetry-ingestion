package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	envEndpoint     = "EYESECURITY_ENDPOINT"
	envTimeout      = "EYESECURITY_TIMEOUT"
	defaultEndpoint = "http://localhost:8080/ingest"

	upstreamBatchSize   = 20
	upstreamRateWindow  = 10 * time.Second
	timeoutSlack        = 30 * time.Second
	timeoutSlackPercent = 25
	estimateRounding    = time.Second
)

func estimateDuration(records int) time.Duration {
	batches := (records + upstreamBatchSize - 1) / upstreamBatchSize
	if batches < 1 {
		return 0
	}
	return time.Duration(batches-1) * upstreamRateWindow
}

func estimateTimeout(records int) time.Duration {
	floor := estimateDuration(records)
	return floor + floor*timeoutSlackPercent/100 + timeoutSlack
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
