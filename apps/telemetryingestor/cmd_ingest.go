package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	icsv "github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/filter"
)

const ndjsonContentType = "application/x-ndjson"

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
	Type string `json:"type"`

	Received         int `json:"received"`
	Enriched         int `json:"enriched"`
	Ingested         int `json:"ingested"`
	FailedEnrichment int `json:"failedEnrichment"`
	FailedCategory   int `json:"failedCategory"`
	FailedAnalytics  int `json:"failedAnalytics"`

	// accounted is false when the processor accepted the request but returned
	// a body we could not read, so its per-record outcome is unknown.
	accounted bool
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
	if *file == "" {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "\nerror: -file is required")
		return 2
	}

	// Resolve configuration precedence: flag > env > default.
	resolvedEndpoint := resolveEndpoint(*endpoint, flagWasSet(fs, "endpoint"))

	flt, err := filter.Parse(where)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	// The derived timeout needs the record count, so it is resolved after the
	// file is read. Check the explicit values now so a bad flag is a usage
	// error rather than something reported halfway through a run.
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
	if !flt.Empty() {
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
	records, err := readRecords(*file, flt, &stats)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	// The timeout has to cover the upstream rate limit, which depends on how
	// many records we are actually sending -- hence resolving it here.
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
	res, err := postRecords(&http.Client{Timeout: resolvedTimeout}, resolvedEndpoint, records,
		func(ev ingestResult) { fmt.Println(progressLine(start, len(records), ev)) })
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
		fmt.Println("  done")
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

// readRecords streams the CSV once, applying the filter and counting the rows
// it had to skip. The whole file is held in memory: at ~60 bytes per record
// even a million rows is small next to the ~8 hours the rate limit would need
// to send them.
func readRecords(path string, flt filter.Filter, stats *ingestStats) ([]ingestRecord, error) {
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
		// Every row the reader hands back counts as read, malformed included,
		// so the summary adds up: read == filtered + skipped + sent.
		stats.read++
		if err != nil {
			if _, ok := err.(*icsv.FieldCountError); ok {
				stats.skippedMalformed++
				continue
			}
			return nil, err
		}

		if !flt.Match(rec) {
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

func postRecords(client *http.Client, endpoint string, records []ingestRecord, onProgress func(ingestResult)) (ingestResult, error) {
	body, err := json.Marshal(ingestRequest{Records: records})
	if err != nil {
		return ingestResult{}, fmt.Errorf("marshalling request: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return ingestResult{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", ndjsonContentType+", application/json")

	resp, err := client.Do(req)
	if err != nil {
		return ingestResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return ingestResult{}, fmt.Errorf("processor returned %s: %s", resp.Status, bytes.TrimSpace(snippet))
	}

	if strings.Contains(resp.Header.Get("Content-Type"), ndjsonContentType) {
		return readProgressStream(resp.Body, onProgress)
	}

	var res ingestResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		// Accepted, but we cannot say what happened to the individual records.
		return ingestResult{Received: len(records)}, nil
	}
	res.accounted = true
	return res, nil
}

func readProgressStream(body io.Reader, onProgress func(ingestResult)) (ingestResult, error) {
	dec := json.NewDecoder(body)

	var final ingestResult
	for {
		var ev ingestResult
		err := dec.Decode(&ev)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ingestResult{}, fmt.Errorf("%w: %v", errStreamTruncated, err)
		}

		switch ev.Type {
		case "result":
			ev.accounted = true
			final = ev
		case "progress":
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

	// Extrapolating from the observed rate self-corrects; the up-front estimate
	// assumes perfectly full batches, which a lossy run never achieves.
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
}

// statLabelWidth must be >= the longest label below ("dropped (enrichment):")
// so every value lands in the same column.
const statLabelWidth = 22

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

	exit := 0
	if s.failed > 0 {
		printStat("failed:", s.failed)
		exit = 1
	}
	if s.unknown > 0 {
		printStat("outcome unknown:", s.unknown)
		fmt.Println("\nThe request timed out but may still have been ingested. Re-run with a\n" +
			"longer -timeout, and expect duplicates for these records.")
		exit = 1
	}
	if s.ingested < s.accepted-s.unaccounted && exit == 0 {
		exit = 1
	}
	return exit
}
