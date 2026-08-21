package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	icsv "github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/filter"
)

const ndjsonContentType = "application/x-ndjson"

type frameKind string

const (
	frameProgress frameKind = "progress"
	frameResult   frameKind = "result"
)

var errStreamTruncated = errors.New("progress stream ended before the final result")

type ingestRecord struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

type ingestRequest struct {
	Records []ingestRecord `json:"records"`
}

type ingestResult struct {
	Kind frameKind `json:"type"`

	Received         int `json:"received"`
	Enriched         int `json:"enriched"`
	Ingested         int `json:"ingested"`
	FailedEnrichment int `json:"failedEnrichment"`
	FailedCategory   int `json:"failedCategory"`
	FailedAnalytics  int `json:"failedAnalytics"`

	RecordErrors []recordError `json:"recordErrors,omitempty"`

	accounted bool
}

type recordError struct {
	ID     int64  `json:"id,omitempty"`
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

func runIngest(args []string) int {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	file := fs.String("file", "", "path to the CSV file to ingest (required)")
	endpoint := fs.String("endpoint", defaultEndpoint, "processor ingest endpoint (env "+envEndpoint+")")
	timeout := fs.Duration("timeout", 0, "HTTP timeout; default is derived from the number of records (env "+envTimeout+")")
	dryRun := fs.Bool("dry-run", false, "parse and filter but do not POST anything")
	skipPreflight := fs.Bool("skip-preflight", false, "do not health-check the processor before ingesting")
	var where stringSlice
	fs.Var(&where, "where", "filter as column=value (exact) or column~value (contains); repeatable, ANDed")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Ingest a telemetry CSV and POST its records to the processor.\n\nUsage:\n  telemetryingestor ingest -file <path> [-endpoint <url>] [-timeout <dur>] [-where <expr>] [-dry-run] [-skip-preflight]\n\nFlags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *file == "" {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "\nerror: -file is required")
		return 2
	}

	// Resolve configuration precedence: flag > env > default.
	resolvedEndpoint := resolveEndpoint(*endpoint, flagWasSet(fs, "endpoint"))

	rowFilter, err := filter.Parse(where)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	timeoutSet := flagWasSet(fs, "timeout")
	if _, err := resolveTimeout(*timeout, timeoutSet, 0); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	if !*dryRun && !*skipPreflight {
		probe := &http.Client{Timeout: defaultPreflightTimeout}
		if err := checkProcessorUp(probe, resolvedEndpoint); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
	}

	fmt.Printf("Ingesting %s\n", *file)
	fmt.Printf("  endpoint:   %s\n", resolvedEndpoint)
	if !rowFilter.Empty() {
		fmt.Printf("  filter:     %s\n", where.String())
	}
	if *dryRun {
		fmt.Println("  mode:       dry-run (no records will be sent)")
	} else if *skipPreflight {
		fmt.Println("  mode:       pre-check skipped")
	} else {
		fmt.Println("  processor:  reachable and healthy")
	}

	stats := ingestStats{}
	records, err := readRecords(ctx, *file, rowFilter, &stats)
	switch {
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(os.Stderr, "\ninterrupted before sending; nothing was ingested")
		return 1
	case err != nil:
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	resolvedTimeout, _ := resolveTimeout(*timeout, timeoutSet, len(records))

	if len(records) == 0 {
		fmt.Println("\nNothing to send.")
		return stats.report(*dryRun)
	}

	if *dryRun {
		stats.accepted = len(records)
		stats.ingested = len(records)
		return stats.report(true)
	}

	fmt.Printf("  records:    %d\n", len(records))
	fmt.Printf("  estimated:  %s at %d records / %s (upstream rate limit)\n",
		estimateDuration(len(records)).Round(estimateRounding), upstreamBatchSize, upstreamRateWindow)
	fmt.Printf("  timeout:    %s\n\n", resolvedTimeout)
	fmt.Println("  sending...")

	start := time.Now()
	var lastProgress ingestResult
	res, err := postRecords(ctx, &http.Client{Timeout: resolvedTimeout}, resolvedEndpoint, records,
		func(ev ingestResult) {
			lastProgress = ev
			fmt.Println(progressLine(start, len(records), ev))
		})
	switch {
	case err == nil && !res.accounted:
		stats.accepted = len(records)
		stats.unaccounted = len(records)
		fmt.Println("  accepted (processor returned no per-record counts)")
	case err == nil:
		stats.accepted = len(records)
		stats.ingested = res.Ingested
		stats.droppedCategory = res.FailedCategory
		stats.droppedEnrichment = res.FailedEnrichment
		stats.droppedAnalytics = res.FailedAnalytics
		stats.recordErrors = res.RecordErrors
		fmt.Println("  done")
	case errors.Is(err, context.Canceled):
		stats.accepted = len(records)
		stats.ingested = lastProgress.Ingested
		stats.unknown = len(records) - lastProgress.Ingested
		stats.interrupted = true
		fmt.Fprintln(os.Stderr, "\n  interrupted")
	case isTimeout(err) || errors.Is(err, errStreamTruncated):
		// The processor may well have finished the work after we hung up:
		// calling this "failed" would be a lie, and a blind retry would
		// duplicate records upstream.
		stats.unknown = len(records)
		fmt.Fprintf(os.Stderr, "  outcome unknown: %v\n", err)
	default:
		stats.failed = len(records)
		fmt.Fprintf(os.Stderr, "  failed: %v\n", err)
	}

	return stats.report(false)
}

const ctxCheckInterval = 512

func readRecords(ctx context.Context, path string, rowFilter filter.Filter, stats *ingestStats) ([]ingestRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := icsv.NewReader(f)
	if _, err := rd.ReadHeader(); err != nil {
		return nil, err
	}

	var records []ingestRecord
	for {
		rec, err := rd.Next()
		if err == io.EOF {
			break
		}
		stats.read++
		if stats.read%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if err != nil {
			var fieldCount *icsv.FieldCountError
			if errors.As(err, &fieldCount) {
				stats.skippedMalformed++
				continue
			}
			return nil, err
		}

		if !rowFilter.Match(rec) {
			stats.filteredOut++
			continue
		}

		ir, ok := toIngestRecord(rec)
		if !ok {
			stats.skippedInvalid++
			fmt.Fprintf(os.Stderr, "  skipping line %d: invalid id %q\n", rec.Line, rec.ID)
			continue
		}
		records = append(records, ir)
	}
	return records, nil
}

func toIngestRecord(r icsv.Record) (ingestRecord, bool) {
	id, err := strconv.ParseInt(r.ID, 10, 64)
	if err != nil {
		return ingestRecord{}, false
	}
	return ingestRecord{
		ID:       id,
		Asset:    r.Asset,
		IP:       r.IP,
		Category: r.Category,
	}, true
}

func postRecords(ctx context.Context, client *http.Client, endpoint string, records []ingestRecord, onProgress func(ingestResult)) (ingestResult, error) {
	body, err := json.Marshal(ingestRequest{Records: records})
	if err != nil {
		return ingestResult{}, fmt.Errorf("marshalling request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return ingestResult{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", ndjsonContentType+", application/json")

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ingestResult{}, ctx.Err()
		}
		return ingestResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return ingestResult{}, fmt.Errorf("processor returned %s: %s", resp.Status, bytes.TrimSpace(snippet))
	}

	if strings.Contains(resp.Header.Get("Content-Type"), ndjsonContentType) {
		return readProgressStream(ctx, resp.Body, onProgress)
	}

	var res ingestResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		// Accepted, but we cannot say what happened to the individual records.
		return ingestResult{Received: len(records)}, nil
	}
	res.accounted = true
	return res, nil
}

func readProgressStream(ctx context.Context, body io.Reader, onProgress func(ingestResult)) (ingestResult, error) {
	dec := json.NewDecoder(body)

	var final ingestResult
	for {
		var ev ingestResult
		err := dec.Decode(&ev)
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ingestResult{}, ctxErr
			}
			return ingestResult{}, fmt.Errorf("%w: %v", errStreamTruncated, err)
		}

		switch ev.Kind {
		case frameResult:
			ev.accounted = true
			final = ev
		case frameProgress:
			if onProgress != nil {
				onProgress(ev)
			}
		}
	}

	if !final.accounted {
		return ingestResult{}, errStreamTruncated
	}
	return final, nil
}

func progressLine(start time.Time, total int, ev ingestResult) string {
	elapsed := time.Since(start)
	width := len(strconv.Itoa(total))

	var pct float64
	if total > 0 {
		pct = float64(ev.Ingested) / float64(total) * 100
	}

	eta := estimateDuration(total) - elapsed
	if ev.Ingested > 0 {
		eta = (elapsed / time.Duration(ev.Ingested)) * time.Duration(total-ev.Ingested)
	}
	if eta < 0 {
		eta = 0
	}

	return fmt.Sprintf("  [%*d/%d] %5.1f%% | enriched %d | elapsed %s | eta %s",
		width, ev.Ingested, total, pct, ev.Enriched,
		elapsed.Round(estimateRounding), eta.Round(estimateRounding))
}

type ingestStats struct {
	read              int
	filteredOut       int
	skippedMalformed  int
	skippedInvalid    int
	accepted          int
	ingested          int
	unaccounted       int
	droppedCategory   int
	droppedEnrichment int
	droppedAnalytics  int
	failed            int
	unknown           int
	interrupted       bool
	recordErrors      []recordError
}

const maxPrintedErrors = 20

const statLabelWidth = 22

func (s ingestStats) reportRecordErrors() {
	if len(s.recordErrors) == 0 {
		return
	}

	dropped := s.droppedCategory + s.droppedEnrichment + s.droppedAnalytics
	fmt.Printf("\nFailures (%d of %d):\n", min(len(s.recordErrors), maxPrintedErrors), dropped)
	for i, e := range s.recordErrors {
		if i == maxPrintedErrors {
			fmt.Printf("  ... and %d more; see the processor log for the full list\n",
				dropped-maxPrintedErrors)
			break
		}
		fmt.Printf("  record %d [%s] %s\n", e.ID, e.Stage, e.Reason)
	}
	if dropped > len(s.recordErrors) && len(s.recordErrors) < maxPrintedErrors {
		fmt.Printf("  (processor reported only the first %d of %d failures)\n",
			len(s.recordErrors), dropped)
	}
}

func printStat(label string, value int) {
	fmt.Printf("  %-*s%d\n", statLabelWidth, label, value)
}

func (s ingestStats) report(dryRun bool) int {
	verb := "sent:"
	if dryRun {
		verb = "would send:"
	}

	fmt.Println("\nSummary:")
	printStat("rows read:", s.read)
	if s.filteredOut > 0 {
		printStat("filtered out:", s.filteredOut)
	}
	if s.skippedMalformed > 0 {
		printStat("skipped (malformed):", s.skippedMalformed)
	}
	if s.skippedInvalid > 0 {
		printStat("skipped (bad id):", s.skippedInvalid)
	}
	printStat(verb, s.accepted)
	if !dryRun && s.unaccounted < s.accepted {
		printStat("ingested:", s.ingested)
	}
	if s.unaccounted > 0 {
		printStat("unconfirmed:", s.unaccounted)
	}
	if s.droppedCategory > 0 {
		printStat("dropped (category):", s.droppedCategory)
	}
	if s.droppedEnrichment > 0 {
		printStat("dropped (enrichment):", s.droppedEnrichment)
	}
	if s.droppedAnalytics > 0 {
		printStat("dropped (analytics):", s.droppedAnalytics)
	}

	s.reportRecordErrors()

	exit := 0
	if s.failed > 0 {
		printStat("failed:", s.failed)
		exit = 1
	}
	if s.unknown > 0 {
		printStat("outcome unknown:", s.unknown)
		if s.interrupted {
			fmt.Println("\nInterrupted. The counts above are the last the processor confirmed; a\n" +
				"few records already queued upstream may still land after this. Re-running\n" +
				"will duplicate whatever did.")
		} else {
			fmt.Println("\nThe request timed out but may still have been ingested. Re-run with a\n" +
				"longer -timeout, and expect duplicates for these records.")
		}
		exit = 1
	}
	if s.ingested < s.accepted-s.unaccounted && exit == 0 {
		exit = 1
	}
	return exit
}
