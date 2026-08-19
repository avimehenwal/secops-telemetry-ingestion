package validate

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
)

const dateLayout = "02/01/2006 15:04"

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

func checkRecord(rep *Report, r csv.Record) {
	if r.ID == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColID, Severity: SeverityError, Message: "missing id"})
	} else if _, err := strconv.ParseInt(r.ID, 10, 64); err != nil {
		rep.add(Issue{Line: r.Line, Column: csv.ColID, Severity: SeverityError, Message: fmt.Sprintf("id %q is not an integer", r.ID)})
	}

	if r.IP == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColIP, Severity: SeverityError, Message: "missing ip"})
	} else if net.ParseIP(r.IP) == nil {
		rep.add(Issue{Line: r.Line, Column: csv.ColIP, Severity: SeverityError, Message: fmt.Sprintf("%q is not a valid IP address", r.IP)})
	}

	if r.Asset == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColAsset, Severity: SeverityError, Message: "missing asset_name"})
	}

	if r.Category == "" {
		rep.add(Issue{Line: r.Line, Column: csv.ColCategory, Severity: SeverityWarning, Message: "empty category"})
	}

	if r.Source == "" || r.Source == "null" {
		rep.add(Issue{Line: r.Line, Column: csv.ColSource, Severity: SeverityWarning, Message: fmt.Sprintf("suspicious source %q", r.Source)})
	}

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
