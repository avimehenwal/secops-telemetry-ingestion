package main

import (
	"flag"
	"fmt"
	"os"
)

type config struct {
	csvPath        string
	processorURL   string
	categoryFilter string
}

const usageText = `telemetryIngestor Tool ingests a CSV file of secOps telemetry logs and processes them via microservice.

Usage:
  telemetryIngestor -file <path> [-endpoint <url>] [-category <name>]

Flags:
`

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("telemetryIngestor", flag.ContinueOnError)

	csvPath := fs.String("file", "", "path to the CSV file to ingest (required)")
	processorURL := fs.String("endpoint", "http://localhost:8080/ingest", "URL of the telemetryprocessor ingest endpoint")
	categoryFilter := fs.String("category", "", "optional MITRE category to filter records by before ingesting")

	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usageText)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	if *csvPath == "" {
		fs.Usage()
		return config{}, fmt.Errorf("-file is required")
	}

	return config{
		csvPath:        *csvPath,
		processorURL:   *processorURL,
		categoryFilter: *categoryFilter,
	}, nil
}

func run(cfg config) error {
	// TODO: open and parse cfg.csvPath into records.
	// TODO: if cfg.categoryFilter is set, only keep matching records.
	// TODO: POST the records to cfg.processorURL and report per-record
	//       success/failure feedback to the user (stdout/exit code).
	fmt.Printf("telemetryIngestor scaffold: would ingest %q into %q", cfg.csvPath, cfg.processorURL)
	if cfg.categoryFilter != "" {
		fmt.Printf(" filtered by category %q", cfg.categoryFilter)
	}
	fmt.Println()
	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
