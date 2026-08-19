package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultPreflightTimeout = 5 * time.Second

func healthURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid endpoint %q: want an absolute URL such as http://localhost:8080/ingest", endpoint)
	}
	u.Path = strings.TrimSuffix(u.Path, "/ingest") + "/health"
	u.Path = strings.Replace(u.Path, "//health", "/health", 1)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

type preflightError struct {
	url  string
	hint string
	err  error
}

func (e *preflightError) Error() string {
	return fmt.Sprintf("processor pre-check failed for %s: %v\n\n%s", e.url, e.err, e.hint)
}

func (e *preflightError) Unwrap() error { return e.err }

func checkProcessorUp(client *http.Client, endpoint string) error {
	health, err := healthURL(endpoint)
	if err != nil {
		return err
	}

	resp, err := client.Get(health)
	if err != nil {
		return &preflightError{url: health, hint: transportHint(endpoint, err), err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &preflightError{
			url: health,
			hint: "The processor answered but reports itself unhealthy. Check its logs, and\n" +
				"that its own dependencies (Enrichment, Analytics) are reachable.",
			err: fmt.Errorf("health endpoint returned %s", resp.Status),
		}
	}
	return nil
}

func transportHint(endpoint string, err error) string {
	switch {
	case isTimeout(err):
		return "The processor did not respond in time. It may be starting up or overloaded;\n" +
			"retry shortly, or pass -skip-preflight to attempt the ingest anyway."
	case isDNSError(err):
		return fmt.Sprintf("The host in %s could not be resolved. Check the URL for typos,\n"+
			"or set %s to the correct address.", endpoint, envEndpoint)
	default:
		return fmt.Sprintf("Nothing is listening at %s. Start the processor, for example:\n\n"+
			"  go run ./apps/telemetryprocessor/cmd/server\n\n"+
			"then re-run this command. Use -endpoint or %s to point at a different\n"+
			"instance, or -dry-run to parse the CSV without sending anything.", endpoint, envEndpoint)
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isDNSError(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}
