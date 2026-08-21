package csv

import (
	"io"
	"strings"
	"testing"
)

const header = "id;asset_name;ip;created_utc;source;category\n"

func TestBareQuoteDoesNotAbortTheFile(t *testing.T) {
	src := header +
		`1;DC"01;10.0.0.1;01/01/2024 10:00;defender;phising` + "\n" +
		`2;NB0483D;10.0.0.2;01/01/2024 10:01;defender;phising` + "\n"

	r := NewReader(strings.NewReader(src))
	if _, err := r.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	var ids []string
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("row %d: %v", len(ids)+1, err)
		}
		ids = append(ids, rec.ID)
	}
	if len(ids) != 2 {
		t.Fatalf("got %d rows (%v), want both", len(ids), ids)
	}
}

func TestFieldCountErrorIsRecoverable(t *testing.T) {
	src := header +
		"1;DC01;10.0.0.1\n" +
		"2;NB0483D;10.0.0.2;01/01/2024 10:01;defender;phising\n"

	r := NewReader(strings.NewReader(src))
	if _, err := r.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	rec, err := r.Next()
	fce, ok := err.(*FieldCountError)
	if !ok {
		t.Fatalf("want *FieldCountError, got %T (%v)", err, err)
	}
	if fce.Line != 2 || fce.Got != 3 || fce.Want != len(Header) {
		t.Errorf("unexpected %+v", fce)
	}
	if rec.ID != "1" || rec.Category != "" {
		t.Errorf("short row should be padded, got %+v", rec)
	}

	if rec, err = r.Next(); err != nil {
		t.Fatalf("reader should continue after a bad row: %v", err)
	}
	if rec.ID != "2" || rec.Category != "phising" {
		t.Errorf("got %+v", rec)
	}
}

func TestUnexpectedHeaderIsRejected(t *testing.T) {
	r := NewReader(strings.NewReader("id;asset;ip\n1;DC01;10.0.0.1\n"))
	if _, err := r.ReadHeader(); err == nil {
		t.Fatal("expected an error for a header that is not the documented one")
	}
}
