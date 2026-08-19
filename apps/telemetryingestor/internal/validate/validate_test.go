package validate

import (
	"strings"
	"testing"
)

func TestRunRejectsBadHeader(t *testing.T) {
	_, err := Run(strings.NewReader("a;b;c\n1;2;3\n"))
	if err == nil {
		t.Fatal("expected header error")
	}
}

func TestRunClassifiesIssues(t *testing.T) {
	const data = `id;asset_name;ip;created_utc;source;category
119611;server_horizon;102.145.229.227;27/02/2024 00:00;pxtrpf;phising
notanint;asset;1.2.3.4;27/02/2024 00:00;defender;phising
315061;server_spray;999.999.0.1;27/02/2024 00:00;crowdstrike;phising
402618;server_spark;8.245.154.104;bad-date;null;
`
	rep, err := Run(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Rows != 4 {
		t.Fatalf("Rows=%d, want 4", rep.Rows)
	}
	// Errors: non-integer id, invalid IP. Warnings: empty category, null
	// source, unparseable timestamp on the last row.
	if rep.Errors != 2 {
		t.Fatalf("Errors=%d, want 2 (%v)", rep.Errors, rep.Issues)
	}
	if rep.OK() {
		t.Fatal("report should not be OK with errors present")
	}
	if rep.Warnings != 3 {
		t.Fatalf("Warnings=%d, want 3 (%v)", rep.Warnings, rep.Issues)
	}
}

func TestRunOnCleanData(t *testing.T) {
	const data = `id;asset_name;ip;created_utc;source;category
119611;server_horizon;102.145.229.227;27/02/2024 00:00;pxtrpf;phising
`
	rep, err := Run(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.Warnings != 0 {
		t.Fatalf("expected clean report, got %+v", rep)
	}
}
