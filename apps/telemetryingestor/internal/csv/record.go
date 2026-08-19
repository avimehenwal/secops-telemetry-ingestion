// Package csv reads the secOps telemetry CSV format described in the tech
// assessment: a ';'-delimited file with a fixed header of six columns.
//
// The sample data (docs/example_data_2.csv) is messy on purpose — it mixes
// delimiters styles, has blank/`null` fields and free-text categories — so
// the reader is deliberately lenient about row arity and leaves judgement of
// "is this row usable" to the validate package. That keeps parsing (structural)
// separate from validation (semantic).
package csv

import "strings"

// Column names, in the exact order they appear in the CSV header.
const (
	ColID         = "id"
	ColAsset      = "asset_name"
	ColIP         = "ip"
	ColCreatedUTC = "created_utc"
	ColSource     = "source"
	ColCategory   = "category"
)

// Header is the canonical column order expected in the CSV file.
var Header = []string{ColID, ColAsset, ColIP, ColCreatedUTC, ColSource, ColCategory}

// Columns returns the list of column names records can be filtered on.
func Columns() []string { return append([]string(nil), Header...) }

// Record is one data row from the CSV file. Line is the 1-based line number in
// the source file (the header is line 1), which lets callers point users at the
// exact offending row when reporting problems.
type Record struct {
	Line       int
	ID         string
	Asset      string
	IP         string
	CreatedUTC string
	Source     string
	Category   string
}

// Field returns the value of the named column (case-insensitive) so that
// generic tooling — the filter, for example — can address columns by name
// without hard-coding struct field access. The bool reports whether the column
// name is known.
func (r Record) Field(name string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case ColID:
		return r.ID, true
	case ColAsset:
		return r.Asset, true
	case ColIP:
		return r.IP, true
	case ColCreatedUTC:
		return r.CreatedUTC, true
	case ColSource:
		return r.Source, true
	case ColCategory:
		return r.Category, true
	default:
		return "", false
	}
}
