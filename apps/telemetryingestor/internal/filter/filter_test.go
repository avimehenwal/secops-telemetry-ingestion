package filter

import (
	"testing"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryingestor/internal/csv"
)

func rec() csv.Record {
	return csv.Record{ID: "1", Asset: "server_horizon", IP: "1.2.3.4", Source: "defender", Category: "Phising"}
}

func TestParseRejectsUnknownColumn(t *testing.T) {
	if _, err := Parse([]string{"nope=x"}); err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestParseRejectsMalformedExpr(t *testing.T) {
	if _, err := Parse([]string{"category"}); err == nil {
		t.Fatal("expected error for expression without operator")
	}
}

func TestEmptyFilterMatchesEverything(t *testing.T) {
	f, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Empty() || !f.Match(rec()) {
		t.Fatal("empty filter should match everything")
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		name  string
		exprs []string
		want  bool
	}{
		{"exact case-insensitive", []string{"category=phising"}, true},
		{"exact mismatch", []string{"category=exploit"}, false},
		{"contains", []string{"category~phis"}, true},
		{"contains case-insensitive", []string{"asset_name~HORIZON"}, true},
		{"AND both true", []string{"source=defender", "category~phis"}, true},
		{"AND one false", []string{"source=crowdstrike", "category~phis"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Parse(tc.exprs)
			if err != nil {
				t.Fatal(err)
			}
			if got := f.Match(rec()); got != tc.want {
				t.Fatalf("Match=%v, want %v", got, tc.want)
			}
		})
	}
}
