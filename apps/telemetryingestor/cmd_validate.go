package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/validate"
)

func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	file := fs.String("file", "", "path to the CSV file to validate (required)")
	quiet := fs.Bool("quiet", false, "only print the summary, not each issue")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Validate a telemetry CSV and report issues.\n\nUsage:\n  telemetryingestor validate -file <path> [-quiet]\n\nFlags:\n")
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

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	defer f.Close()

	rep, err := validate.Run(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	if !*quiet {
		for _, issue := range rep.Issues {
			fmt.Println(issue)
		}
	}

	fmt.Printf("\nValidated %d rows: %d error(s), %d warning(s).\n", rep.Rows, rep.Errors, rep.Warnings)
	if rep.OK() {
		fmt.Println("CSV is valid for ingestion (warnings do not block ingestion).")
		return 0
	}
	fmt.Println("CSV has blocking errors; fix them before ingesting.")
	return 1
}
