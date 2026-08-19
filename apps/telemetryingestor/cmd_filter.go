package main

import (
	stdcsv "encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"

	icsv "github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/filter"
)

func runFilter(args []string) int {
	fs := flag.NewFlagSet("filter", flag.ContinueOnError)
	file := fs.String("file", "", "path to the CSV file to filter (required)")
	var where stringSlice
	fs.Var(&where, "where", "filter as column=value (exact) or column~value (contains); repeatable, ANDed")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Print rows matching a filter, as CSV, to stdout.\n\nUsage:\n  telemetryingestor filter -file <path> -where <expr> [-where <expr> ...]\n\nExamples:\n  telemetryingestor filter -file data.csv -where category~phis\n  telemetryingestor filter -file data.csv -where source=defender -where category=phising\n\nFlags:\n")
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

	w := stdcsv.NewWriter(os.Stdout)
	w.Comma = ';'
	_ = w.Write(icsv.Header)

	var matched, total int
	for {
		rec, err := rd.Next()
		if err == io.EOF {
			break
		}
		// Tolerate malformed rows: skip arity errors, abort on real read errors.
		if err != nil {
			if _, ok := err.(*icsv.FieldCountError); ok {
				continue
			}
			w.Flush()
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		total++
		if flt.Match(rec) {
			matched++
			_ = w.Write([]string{rec.ID, rec.Asset, rec.IP, rec.CreatedUTC, rec.Source, rec.Category})
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		fmt.Fprintln(os.Stderr, "error writing output:", err)
		return 1
	}

	// Feedback goes to stderr so it does not corrupt the CSV on stdout.
	fmt.Fprintf(os.Stderr, "matched %d of %d rows\n", matched, total)
	return 0
}
