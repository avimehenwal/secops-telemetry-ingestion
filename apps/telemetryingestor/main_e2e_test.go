package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ingestor-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	binPath = filepath.Join(dir, "telemetryingestor")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e build:", err)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	stdout string
	stderr string
	code   int
}

func run(t *testing.T, env []string, args ...string) result {
	t.Helper()

	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	code := 0
	if err := cmd.Run(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("running CLI: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func writeCSV(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telemetry.csv")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}
	return path
}

const csvHeader = "id;asset_name;ip;created_utc;source;category\n"

const sampleCSV = csvHeader +
	"1;laptop-01;10.0.0.1;2024-01-01T00:00:00Z;edr;phishing\n" +
	"2;laptop-02;10.0.0.2;2024-01-01T00:01:00Z;edr;malware\n" +
	"3;server-01;10.0.0.3;2024-01-01T00:02:00Z;siem;phishing\n"

type fakeProcessor struct {
	*httptest.Server

	mu      sync.Mutex
	batches [][]ingestRecord
	status  int
}

func newFakeProcessor(t *testing.T, status int) *fakeProcessor {
	t.Helper()
	p := &fakeProcessor{status: status}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		var req ingestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.batches = append(p.batches, req.Records)
		p.mu.Unlock()
		w.WriteHeader(p.status)
		if p.status >= 200 && p.status < 300 {
			_ = json.NewEncoder(w).Encode(ingestResult{
				Received: len(req.Records),
				Enriched: len(req.Records),
				Ingested: len(req.Records),
			})
		}
	}))
	t.Cleanup(p.Close)
	return p
}

func (p *fakeProcessor) received() [][]ingestRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batches
}

func (p *fakeProcessor) endpoint() string { return p.URL + "/ingest" }

func TestIngestPostsRecordsInChunks(t *testing.T) {
	proc := newFakeProcessor(t, http.StatusOK)
	csv := writeCSV(t, sampleCSV)

	got := run(t, nil, "ingest", "-file", csv, "-endpoint", proc.endpoint(), "-chunk-size", "2")

	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
	}
	batches := proc.received()
	if len(batches) != 2 {
		t.Fatalf("got %d batches, want 2 (chunk-size 2 over 3 records)", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Errorf("batch sizes = %d,%d; want 2,1", len(batches[0]), len(batches[1]))
	}
	want := ingestRecord{ID: 1, Asset: "laptop-01", IP: "10.0.0.1", Category: "phishing"}
	if batches[0][0] != want {
		t.Errorf("first record = %+v, want %+v", batches[0][0], want)
	}
}

func TestIngestFilterIsAppliedBeforeSending(t *testing.T) {
	proc := newFakeProcessor(t, http.StatusOK)
	csv := writeCSV(t, sampleCSV)

	got := run(t, nil, "ingest", "-file", csv, "-endpoint", proc.endpoint(), "-where", "category=phishing")

	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
	}
	batches := proc.received()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("got %v, want a single batch of the 2 phishing records", batches)
	}
	for _, rec := range batches[0] {
		if rec.Category != "phishing" {
			t.Errorf("record %+v leaked past the filter", rec)
		}
	}
}

func TestEndpointPrecedence(t *testing.T) {
	t.Run("env is used when the flag is absent", func(t *testing.T) {
		proc := newFakeProcessor(t, http.StatusOK)
		csv := writeCSV(t, sampleCSV)

		got := run(t, []string{envEndpoint + "=" + proc.endpoint()}, "ingest", "-file", csv)

		if got.code != 0 {
			t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
		}
		if len(proc.received()) == 0 {
			t.Error("processor received nothing; the env endpoint was not used")
		}
	})

	t.Run("flag beats env", func(t *testing.T) {
		wanted := newFakeProcessor(t, http.StatusOK)
		ignored := newFakeProcessor(t, http.StatusOK)
		csv := writeCSV(t, sampleCSV)

		got := run(t, []string{envEndpoint + "=" + ignored.endpoint()},
			"ingest", "-file", csv, "-endpoint", wanted.endpoint())

		if got.code != 0 {
			t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
		}
		if len(wanted.received()) == 0 {
			t.Error("the -endpoint flag was not honoured")
		}
		if len(ignored.received()) != 0 {
			t.Error("records went to the env endpoint despite an explicit -endpoint")
		}
	})
}

func TestDryRunSendsNothing(t *testing.T) {
	proc := newFakeProcessor(t, http.StatusOK)
	csv := writeCSV(t, sampleCSV)

	got := run(t, nil, "ingest", "-file", csv, "-endpoint", proc.endpoint(), "-dry-run")

	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
	}
	if n := len(proc.received()); n != 0 {
		t.Errorf("dry run sent %d batches, want 0", n)
	}
	if !strings.Contains(got.stdout, "dry-run") {
		t.Errorf("stdout does not mention dry-run:\n%s", got.stdout)
	}
}

// A processor that rejects the batch must not be reported as a clean run.
func TestIngestFailsWhenProcessorRejects(t *testing.T) {
	proc := newFakeProcessor(t, http.StatusInternalServerError)
	csv := writeCSV(t, sampleCSV)

	got := run(t, nil, "ingest", "-file", csv, "-endpoint", proc.endpoint())

	if got.code == 0 {
		t.Fatalf("exit code = 0, want non-zero on a failed chunk\nstdout: %s", got.stdout)
	}
	if !strings.Contains(got.stderr, "chunk 1 failed") {
		t.Errorf("stderr does not report the failed chunk:\n%s", got.stderr)
	}
}

func TestValidateExitCodes(t *testing.T) {
	tests := []struct {
		name string
		csv  string
		want int
	}{
		{"clean file exits 0", sampleCSV, 0},
		{
			"blocking error exits 1",
			csvHeader + "not-a-number;laptop-01;10.0.0.1;2024-01-01T00:00:00Z;edr;phishing\n",
			1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := run(t, nil, "validate", "-file", writeCSV(t, tc.csv))
			if got.code != tc.want {
				t.Errorf("exit code = %d, want %d\nstdout: %s\nstderr: %s",
					got.code, tc.want, got.stdout, got.stderr)
			}
		})
	}
}

func TestUsageErrorsExitTwo(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no arguments", nil},
		{"unknown command", []string{"frobnicate"}},
		{"ingest without -file", []string{"ingest"}},
		{"validate without -file", []string{"validate", "-file", ""}},
		{"malformed -where", []string{"ingest", "-file", "x.csv", "-where", "nonsense"}},
		{"non-positive -chunk-size", []string{"ingest", "-file", "x.csv", "-chunk-size", "0"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := run(t, nil, tc.args...)
			if got.code != 2 {
				t.Errorf("exit code = %d, want 2\nstderr: %s", got.code, got.stderr)
			}
			if got.stderr == "" {
				t.Error("nothing written to stderr for a usage error")
			}
		})
	}
}

func TestHelpExitsZeroOnStdout(t *testing.T) {
	got := run(t, nil, "help")
	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0", got.code)
	}
	for _, want := range []string{"ingest", "validate", "filter"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("help output does not list %q:\n%s", want, got.stdout)
		}
	}
}

func TestPreflightFailsFastWhenProcessorIsDown(t *testing.T) {
	// A server that is closed immediately gives us a port nothing listens on.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := dead.URL + "/ingest"
	dead.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", endpoint)

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1\nstderr: %s", got.code, got.stderr)
	}
	if !strings.Contains(got.stderr, "pre-check failed") {
		t.Errorf("stderr does not name the pre-check:\n%s", got.stderr)
	}
	if !strings.Contains(got.stderr, "Nothing is listening") {
		t.Errorf("stderr lacks the actionable hint:\n%s", got.stderr)
	}
	// The check must happen before any records are streamed.
	if strings.Contains(got.stdout, "sent ") {
		t.Errorf("records were sent despite a failed pre-check:\n%s", got.stdout)
	}
}

func TestPreflightFailsWhenHealthIsUnhealthy(t *testing.T) {
	var ingests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ingests++
	}))
	defer srv.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", srv.URL+"/ingest")

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1\nstderr: %s", got.code, got.stderr)
	}
	if !strings.Contains(got.stderr, "reports itself unhealthy") {
		t.Errorf("stderr does not explain the unhealthy response:\n%s", got.stderr)
	}
	if ingests != 0 {
		t.Errorf("%d ingest requests were made after a failed pre-check", ingests)
	}
}

func TestPreflightRejectsMalformedEndpoint(t *testing.T) {
	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", "localhost:8080/ingest")

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1\nstderr: %s", got.code, got.stderr)
	}
	if !strings.Contains(got.stderr, "absolute URL") {
		t.Errorf("stderr does not explain the malformed endpoint:\n%s", got.stderr)
	}
}

func TestSkipPreflightAttemptsIngestAnyway(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := dead.URL + "/ingest"
	dead.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", endpoint, "-skip-preflight")

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1 (the chunks still fail)\nstderr: %s", got.code, got.stderr)
	}
	if strings.Contains(got.stderr, "pre-check failed") {
		t.Errorf("-skip-preflight did not skip the pre-check:\n%s", got.stderr)
	}
	if !strings.Contains(got.stderr, "chunk 1 failed") {
		t.Errorf("expected the ingest to be attempted and fail per chunk:\n%s", got.stderr)
	}
}

func TestDryRunSkipsPreflight(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := dead.URL + "/ingest"
	dead.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", endpoint, "-dry-run")

	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0: a dry run needs no processor\nstderr: %s", got.code, got.stderr)
	}
}

// The happy path reports the pre-check so a successful run is self-documenting.
func TestPreflightReportedOnSuccess(t *testing.T) {
	proc := newFakeProcessor(t, http.StatusOK)

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", proc.endpoint())

	if got.code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, "reachable and healthy") {
		t.Errorf("stdout does not report the pre-check:\n%s", got.stdout)
	}
}

func TestHealthURLDerivation(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"http://localhost:8080/ingest", "http://localhost:8080/health"},
		{"http://localhost:8080/", "http://localhost:8080/health"},
		{"http://localhost:8080", "http://localhost:8080/health"},
		{"https://proc.internal/api/ingest", "https://proc.internal/api/health"},
		{"http://localhost:8080/ingest?debug=1", "http://localhost:8080/health"},
	}
	for _, tc := range tests {
		got, err := healthURL(tc.endpoint)
		if err != nil {
			t.Errorf("healthURL(%q) returned error: %v", tc.endpoint, err)
			continue
		}
		if got != tc.want {
			t.Errorf("healthURL(%q) = %q, want %q", tc.endpoint, got, tc.want)
		}
	}

	for _, bad := range []string{"", "localhost:8080/ingest", "/ingest", "://nope"} {
		if got, err := healthURL(bad); err == nil {
			t.Errorf("healthURL(%q) = %q, want an error", bad, got)
		}
	}
}

// The default chunk maps 1:1 onto an upstream Analytics request, and the
// derived timeout must leave room for the rate-limit window each extra
// request costs. A fixed timeout is what made every default run fail.
func TestEstimateTimeoutTracksChunkSize(t *testing.T) {
	tests := []struct {
		chunk int
		want  time.Duration
	}{
		{1, timeoutSlack},
		{20, timeoutSlack},
		{21, upstreamRateWindow + timeoutSlack},
		{100, 4*upstreamRateWindow + timeoutSlack},
	}
	for _, tc := range tests {
		if got := estimateTimeout(tc.chunk); got != tc.want {
			t.Errorf("estimateTimeout(%d) = %s, want %s", tc.chunk, got, tc.want)
		}
	}
	if defaultChunkSize != upstreamBatchSize {
		t.Errorf("defaultChunkSize = %d, want %d so one chunk is one upstream request",
			defaultChunkSize, upstreamBatchSize)
	}
}

// A chunk that times out may still have been ingested. Reporting it as failed
// would be false, and would invite a retry that duplicates records upstream.
func TestIngestReportsTimedOutChunkAsUnknown(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		time.Sleep(500 * time.Millisecond)
	}))
	defer slow.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV),
		"-endpoint", slow.URL+"/ingest", "-timeout", "100ms")

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1\nstdout: %s", got.code, got.stdout)
	}
	if !strings.Contains(got.stderr, "outcome unknown") {
		t.Errorf("timed-out chunk not reported as unknown:\n%s", got.stderr)
	}
	if strings.Contains(got.stderr, "failed (") {
		t.Errorf("a timeout must not be reported as an outright failure:\n%s", got.stderr)
	}
	if !strings.Contains(got.stdout, "outcome unknown:") {
		t.Errorf("summary does not carry the unknown records:\n%s", got.stdout)
	}
}

// A 2xx is not proof of ingestion: the processor's body says how many records
// reached Analytics, and a shortfall must be visible and non-zero-exit.
func TestIngestReportsProcessorSideDrops(t *testing.T) {
	proc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		var req ingestRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(ingestResult{
			Received:       len(req.Records),
			Enriched:       len(req.Records) - 1,
			Ingested:       len(req.Records) - 1,
			FailedCategory: 1,
		})
	}))
	defer proc.Close()

	got := run(t, nil, "ingest", "-file", writeCSV(t, sampleCSV), "-endpoint", proc.URL+"/ingest")

	if got.code != 1 {
		t.Fatalf("exit code = %d, want 1 when records were dropped\nstdout: %s", got.code, got.stdout)
	}
	if !strings.Contains(got.stdout, "ingested:") || !strings.Contains(got.stdout, "dropped (category):") {
		t.Errorf("summary hides the processor-side drop:\n%s", got.stdout)
	}
}
