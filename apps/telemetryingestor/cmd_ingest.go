package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	icsv "github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/filter"
)

// ingestRecord is the per-record shape sent to the processor. It mirrors the
// processor's model.Record (id, asset, ip, category). We keep a local copy
// rather than importing across the module boundary — the processor's model is
// in an internal/ package of a different module and is not importable, and a
// CLI owning its own wire contract is a reasonable seam anyway.
//
// Assumption: the processor is responsible for normalising the free-text
// `category` to its Enrichment enum, so the CLI forwards it verbatim. `source`
// and `created_utc` are not part of the processor contract and are dropped.
type ingestRecord struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

type ingestRequest struct {
	Records []ingestRecord `json:"records"`
}

func runIngest(args []string) int {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	file := fs.String("file", "", "path to the CSV file to ingest (required)")
	endpoint := fs.String("endpoint", defaultEndpoint, "processor ingest endpoint (env "+envEndpoint+")")
	chunkSize := fs.Int("chunk-size", defaultChunkSize, "records to POST per request (env "+envChunkSize+")")
	dryRun := fs.Bool("dry-run", false, "parse and filter but do not POST anything")
	var where stringSlice
	fs.Var(&where, "where", "filter as column=value (exact) or column~value (contains); repeatable, ANDed")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Ingest a telemetry CSV and POST records to the processor.\n\nUsage:\n  telemetryingestor ingest -file <path> [-endpoint <url>] [-chunk-size <n>] [-where <expr>] [-dry-run]\n\nFlags:\n")
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
	resolvedChunk, err := resolveChunkSize(*chunkSize, flagWasSet(fs, "chunk-size"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	flt, err := filter.Parse(where)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	defer f.Close()

	rd := icsv.NewReader(f)
	if _, err := rd.ReadHeader(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	// User feedback up front so the run is self-documenting.
	fmt.Printf("Ingesting %s\n", *file)
	fmt.Printf("  endpoint:   %s\n", resolvedEndpoint)
	fmt.Printf("  chunk size: %d\n", resolvedChunk)
	if !flt.Empty() {
		fmt.Printf("  filter:     %s\n", where.String())
	}
	if *dryRun {
		fmt.Println("  mode:       dry-run (no records will be sent)")
	}
	fmt.Println()

	client := &http.Client{Timeout: 30 * time.Second}
	stats := ingestStats{}
	batch := make([]ingestRecord, 0, resolvedChunk)

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if *dryRun {
			fmt.Printf("  [dry-run] would send %d records\n", len(batch))
			stats.sent += len(batch)
			batch = batch[:0]
			return true
		}
		if err := postChunk(client, resolvedEndpoint, batch); err != nil {
			stats.failedChunks++
			stats.failedRecords += len(batch)
			fmt.Fprintf(os.Stderr, "  chunk failed (%d records): %v\n", len(batch), err)
		} else {
			stats.sent += len(batch)
			fmt.Printf("  sent %d records (%d total)\n", len(batch), stats.sent)
		}
		batch = batch[:0]
		return true
	}

	for {
		rec, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if _, ok := err.(*icsv.FieldCountError); ok {
				stats.skippedMalformed++
				continue
			}
			flush()
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		stats.read++

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

		batch = append(batch, ir)
		if len(batch) >= resolvedChunk {
			flush()
		}
	}
	flush()

	return stats.report(*dryRun)
}

// toIngestRecord converts a CSV record to the wire shape, parsing the id. A
// non-integer id is the one hard requirement that cannot be forwarded, so such
// a record is rejected (reported by the caller).
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

// postChunk POSTs one batch of records and treats any non-2xx as a failure.
func postChunk(client *http.Client, endpoint string, records []ingestRecord) error {
	body, err := json.Marshal(ingestRequest{Records: records})
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("processor returned %s: %s", resp.Status, bytes.TrimSpace(snippet))
	}
	return nil
}

// ingestStats accumulates counters for end-of-run feedback.
type ingestStats struct {
	read             int
	filteredOut      int
	skippedMalformed int
	skippedInvalid   int
	sent             int
	failedRecords    int
	failedChunks     int
}

// report prints a summary and returns the process exit code.
func (s ingestStats) report(dryRun bool) int {
	fmt.Println("\nSummary:")
	fmt.Printf("  rows read:          %d\n", s.read)
	if s.filteredOut > 0 {
		fmt.Printf("  filtered out:       %d\n", s.filteredOut)
	}
	if s.skippedMalformed > 0 {
		fmt.Printf("  skipped (malformed): %d\n", s.skippedMalformed)
	}
	if s.skippedInvalid > 0 {
		fmt.Printf("  skipped (bad id):   %d\n", s.skippedInvalid)
	}
	if dryRun {
		fmt.Printf("  would send:         %d\n", s.sent)
		return 0
	}
	fmt.Printf("  sent:               %d\n", s.sent)
	if s.failedRecords > 0 {
		fmt.Printf("  failed:             %d records in %d chunk(s)\n", s.failedRecords, s.failedChunks)
		return 1
	}
	return 0
}
