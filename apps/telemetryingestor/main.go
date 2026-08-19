// Command telemetryingestor ingests a secOps telemetry CSV file and forwards
// the records to the telemetryprocessor microservice.
//
// It is organised as a small set of subcommands:
//
//	ingest    read a CSV, optionally filter it, and POST records to the processor
//	validate  parse a CSV and report structural/semantic problems
//	filter    apply a filter and print the matching rows back out as CSV
//
// Configuration precedence for shared settings is: CLI flag > environment
// variable > built-in default. See config.go.
package main

import (
	"fmt"
	"os"
)

const rootUsage = `telemetryingestor ingests a secOps telemetry CSV and forwards records to the processor.

Usage:
  telemetryingestor <command> [flags]

Commands:
  ingest      Ingest a CSV and POST records to the processor
  validate    Validate a CSV and report issues without ingesting
  filter      Print rows matching a filter (a dry-run for -where)
  help        Show this help

Run "telemetryingestor <command> -h" for command-specific flags.

Environment:
  EYESECURITY_ENDPOINT     alternative to -endpoint (ingest); -endpoint wins if both set
  EYESECURITY_CHUNK_SIZE   alternative to -chunk-size (ingest); -chunk-size wins if both set
`

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

// dispatch routes to the requested subcommand and returns a process exit code.
// Keeping this as a pure func(args) int makes the whole CLI testable without
// spawning a process.
func dispatch(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, rootUsage)
		return 2
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "ingest":
		return runIngest(rest)
	case "validate":
		return runValidate(rest)
	case "filter":
		return runFilter(rest)
	case "help", "-h", "--help":
		fmt.Print(rootUsage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		fmt.Fprint(os.Stderr, rootUsage)
		return 2
	}
}
