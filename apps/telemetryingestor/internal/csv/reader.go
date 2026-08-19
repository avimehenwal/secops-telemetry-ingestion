package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

type Reader struct {
	raw      *csv.Reader
	header   []string
	haveHead bool
	line     int // number of physical records consumed so far
}

func NewReader(src io.Reader) *Reader {
	cr := csv.NewReader(src)
	cr.Comma = ';'
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	return &Reader{raw: cr}
}

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
