package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

const (
	envEndpoint      = "EYESECURITY_ENDPOINT"
	envChunkSize     = "EYESECURITY_CHUNK_SIZE"
	defaultEndpoint  = "http://localhost:8080/ingest"
	defaultChunkSize = 100
)

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
