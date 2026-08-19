package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

// Environment variables and defaults for configuration that can also be set
// via CLI flags. CLI flags always win when both are present.
const (
	// envEndpoint is the alternative to the -endpoint flag.
	envEndpoint = "EYESECURITY_ENDPOINT"
	// envChunkSize is the alternative to the -chunk-size flag.
	envChunkSize = "EYESECURITY_CHUNK_SIZE"

	defaultEndpoint  = "http://localhost:8080/ingest"
	defaultChunkSize = 100
)

// resolveEndpoint applies the precedence: CLI flag > env var > default.
//
// flagVal is the value the flag package parsed; flagSet reports whether the
// user actually passed -endpoint (as opposed to it holding its default). We
// need flagSet because a flag left at its default is indistinguishable, by
// value alone, from one the user explicitly set to that same string.
func resolveEndpoint(flagVal string, flagSet bool) string {
	if flagSet {
		return flagVal
	}
	if v := os.Getenv(envEndpoint); v != "" {
		return v
	}
	return flagVal // still the default the flag was initialised with
}

// resolveChunkSize applies CLI flag > env var > default, validating that the
// result is a positive integer.
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

// flagWasSet reports whether a flag with the given name was explicitly passed
// on fs. fs.Visit only visits flags that were actually set, which is how we
// distinguish "user passed -endpoint" from "flag holds its default".
func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
