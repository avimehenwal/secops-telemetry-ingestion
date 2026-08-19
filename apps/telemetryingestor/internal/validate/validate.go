// Package validate performs semantic checks on telemetry CSV records and
// reports problems in a way that is friendly to a human at a terminal.
//
// Problems are split into two severities:
//   - Error:   the row cannot be safely ingested (missing key field, bad IP,
//              non-numeric id). These make `validate` exit non-zero.
//   - Warning: the row is usable but suspicious (unparseable timestamp,
//              free-text category that won't map cleanly, `null` source).
//
// This mirrors the reality of the sample data, which is intentionally messy:
// we want to ingest what we reasonably can and loudly flag the rest, rather
// than reject the whole file.
package validate

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
)

// dateLayout is the timestamp format used in the sample data, e.g.
// "27/02/2024 00:00" (day/month/year hour:minute).
const dateLayout = "02/01/2006 15:04"

// Severity distinguishes blocking errors from advisory warnings.
type Severity int

const (
	SeverityWarning Severity = iota
	SeverityError
)

func (s Severity) String() string {
	if s == SeverityError {
		return "error"
	}
	return "warning"
}

// Issue is a single problem found on a single line.
type Issue struct {
	Line     int
	Column   string
	Severity Severity
	Message  string
}

func (i Issue) String() string {
	col := i.Column
	if col == "" {
		col = "-"
	}
	return fmt.Sprintf("line %d [%s] %s: %s", i.Line, i.Severity, col, i.Message)
}

// Report is the outcome of validating a file.
type Report struct {
	Rows     int // data rows seen (excludes header)
	Issues   []Issue
	Errors   int
	Warnings int
}

// OK reports whether the file is free of blocking errors.
func (r Report) OK() bool { return r.Errors == 0 }

func (r *Report) add(i Issue) {
	r.Issues = append(r.Issues, i)
	if i.Severity == SeverityError {
		r.Errors++
	} else {
		r.Warnings++
	}
}

// Run validates the CSV read from src and returns a Report. A header problem is
// returned as a top-level error because nothing else can be trusted after it.
func Run(src io.Reader) (Report, error) {
	rd := csv.NewReader(src)
	if _, err := rd.ReadHeader(); err != nil {
		return Report{}, err
	}

	var rep Report
	for {
		rec, err := rd.Next()
		if err == io.EOF {
			break
		}
		var fce *csv.FieldCountError
		if err != nil {
			if asFieldCount(err, &fce) {
				// Malformed arity: record the issue but still inspect the
				// best-effort fields we did recover.
				rep.add(Issue{Line: fce.Line, Severity: SeverityError, Message: fce.Error()})
			} else {
				return rep, err
			}
		}
		rep.Rows++
		checkRecord(&rep, rec)
	}
	return rep, nil
}

// checkRecord runs all per-record rules.
func checkRecord(rep *Report, r csv.Record) {
	// id: required, must be a positive integer.
	if r.ID == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColID, Severity: SeverityError, Message: "missing id"})
	} else if _, err := strconv.ParseInt(r.ID, 10, 64); err != nil {
		rep.add(Issue{Line: r.Line, Column: csv.ColID, Severity: SeverityError, Message: fmt.Sprintf("id %q is not an integer", r.ID)})
	}

	// ip: required, must be a valid IP address.
	if r.IP == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColIP, Severity: SeverityError, Message: "missing ip"})
	} else if net.ParseIP(r.IP) == nil {
		rep.add(Issue{Line: r.Line, Column: csv.ColIP, Severity: SeverityError, Message: fmt.Sprintf("%q is not a valid IP address", r.IP)})
	}

	// asset_name: required (it is the human-facing label for the event).
	if r.Asset == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColAsset, Severity: SeverityError, Message: "missing asset_name"})
	}

	// category: advisory. Empty is a warning (the record can still be ingested
	// and the processor decides how to handle unknowns); mapping to the
	// Enrichment enum is the processor's job, so we don't reject free text here.
	if r.Category == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColCategory, Severity: SeverityWarning, Message: "empty category"})
	}

	// source: advisory. The sample data uses the literal "null".
	if r.Source == "" || r.Source == "null" {
		rep.add(Issue{Line: r.Line, Column: csv.ColSource, Severity: SeverityWarning, Message: fmt.Sprintf("suspicious source %q", r.Source)})
	}

	// created_utc: advisory. Flag if present but unparseable.
	if r.CreatedUTC != "" {
		if _, err := time.Parse(dateLayout, r.CreatedUTC); err != nil {
			rep.add(Issue{Line: r.Line, Column: csv.ColCreatedUTC, Severity: SeverityWarning, Message: fmt.Sprintf("unparseable timestamp %q (want %q)", r.CreatedUTC, dateLayout)})
		}
	}
}

func asFieldCount(err error, target **csv.FieldCountError) bool {
	fce, ok := err.(*csv.FieldCountError)
	if ok {
		*target = fce
	}
	return ok
}
