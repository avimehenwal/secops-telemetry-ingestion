package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// Reader streams Records out of a telemetry CSV. It wraps the standard library
// csv.Reader configured for the assessment's ';' delimiter. Streaming (rather
// than reading the whole file into memory) is what lets the ingest command
// process arbitrarily large files in bounded memory, one chunk at a time.
type Reader struct {
	raw       *csv.Reader
	header    []string
	haveHead  bool
	line      int // number of physical records consumed so far
}

// NewReader builds a Reader over src.
func NewReader(src io.Reader) *Reader {
	cr := csv.NewReader(src)
	cr.Comma = ';'
	// FieldsPerRecord = -1 disables the stdlib's built-in "every row must have
	// the same number of fields" check. We want malformed rows surfaced as
	// friendly validation issues rather than as a hard parse error that aborts
	// the whole file, so we count fields ourselves.
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	return &Reader{raw: cr}
}

// ReadHeader consumes and returns the first row. It returns an error if the
// header does not match the expected columns, which is a fail-fast signal that
// the caller is looking at the wrong kind of file.
func (r *Reader) ReadHeader() ([]string, error) {
	if r.haveHead {
		return r.header, nil
	}
	row, err := r.raw.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("file is empty: no header row")
	}
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	r.line++
	r.haveHead = true
	r.header = row

	if !headerMatches(row) {
		return row, fmt.Errorf("unexpected header: got %v, want %v", row, Header)
	}
	return row, nil
}

// Next returns the next data record. It returns io.EOF when the file is
// exhausted. ReadHeader is called implicitly if it has not run yet.
//
// A record with the wrong number of fields is not treated as fatal: the fields
// present are mapped positionally and the rest are left blank, so the validate
// command can report a precise "row N has M fields" issue. errFieldCount is
// returned alongside the (best-effort) record so callers can decide.
func (r *Reader) Next() (Record, error) {
	if !r.haveHead {
		if _, err := r.ReadHeader(); err != nil {
			return Record{}, err
		}
	}

	row, err := r.raw.Read()
	if err == io.EOF {
		return Record{}, io.EOF
	}
	if err != nil {
		return Record{}, fmt.Errorf("reading record: %w", err)
	}
	r.line++

	rec := mapRow(r.line, row)
	if len(row) != len(Header) {
		return rec, &FieldCountError{Line: r.line, Got: len(row), Want: len(Header)}
	}
	return rec, nil
}

// FieldCountError reports a row whose column count differs from the header.
type FieldCountError struct {
	Line int
	Got  int
	Want int
}

func (e *FieldCountError) Error() string {
	return fmt.Sprintf("line %d: expected %d fields, got %d", e.Line, e.Want, e.Got)
}

// mapRow maps a raw field slice onto a Record positionally, tolerating short
// or long rows by only reading the fields that exist.
func mapRow(line int, row []string) Record {
	get := func(i int) string {
		if i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}
	return Record{
		Line:       line,
		ID:         get(0),
		Asset:      get(1),
		IP:         get(2),
		CreatedUTC: get(3),
		Source:     get(4),
		Category:   get(5),
	}
}

func headerMatches(row []string) bool {
	if len(row) != len(Header) {
		return false
	}
	for i, want := range Header {
		if !strings.EqualFold(strings.TrimSpace(row[i]), want) {
			return false
		}
	}
	return true
}
