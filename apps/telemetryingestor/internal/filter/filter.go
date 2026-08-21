/*
Package filter implements a small, column-oriented predicate language for
selecting which CSV records to ingest.
*/
package filter

import (
	"fmt"
	"strings"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
)

type op int

const (
	opEquals op = iota
	opContains
)

type predicate struct {
	column string
	value  string
	op     op
}

func (p predicate) match(r csv.Record) bool {
	got, _ := r.Field(p.column) // column existence is validated at Parse time
	got = strings.ToLower(got)
	want := strings.ToLower(p.value)
	switch p.op {
	case opContains:
		return strings.Contains(got, want)
	default:
		return got == want
	}
}

type Filter struct {
	preds []predicate
}

func (f Filter) Empty() bool { return len(f.preds) == 0 }

func (f Filter) Match(r csv.Record) bool {
	for _, p := range f.preds {
		if !p.match(r) {
			return false
		}
	}
	return true
}

func Parse(exprs []string) (Filter, error) {
	var f Filter
	for _, expr := range exprs {
		if strings.TrimSpace(expr) == "" {
			continue
		}
		p, err := parsePredicate(expr)
		if err != nil {
			return Filter{}, err
		}
		if _, ok := (csv.Record{}).Field(p.column); !ok {
			return Filter{}, fmt.Errorf("unknown column %q in filter %q (valid columns: %s)",
				p.column, expr, strings.Join(csv.Columns(), ", "))
		}
		f.preds = append(f.preds, p)
	}
	return f, nil
}

func parsePredicate(expr string) (predicate, error) {
	/*
		'~' is checked before '=' only matters if both appear; we split on the
		first operator character encountered so values may contain the other.
	*/
	if i := strings.IndexByte(expr, '~'); i >= 0 && !containsBefore(expr, '=', i) {
		return predicate{
			column: strings.TrimSpace(expr[:i]),
			value:  strings.TrimSpace(expr[i+1:]),
			op:     opContains,
		}, nil
	}
	if i := strings.IndexByte(expr, '='); i >= 0 {
		return predicate{
			column: strings.TrimSpace(expr[:i]),
			value:  strings.TrimSpace(expr[i+1:]),
			op:     opEquals,
		}, nil
	}
	return predicate{}, fmt.Errorf("invalid filter %q: expected \"column=value\" or \"column~value\"", expr)
}

func containsBefore(s string, b byte, n int) bool {
	i := strings.IndexByte(s, b)
	return i >= 0 && i < n
}
