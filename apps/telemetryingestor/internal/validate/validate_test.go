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

func TestRunReportsShortRowOnceWithoutPhantomWarnings(t *testing.T) {
	const data = `id;asset_name;ip;created_utc;source;category
889807;server_summit;131.21.57.124;02/09/2024 00:00;pxtrpf
`
	rep, err := Run(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Rows != 1 {
		t.Fatalf("Rows=%d, want 1", rep.Rows)
	}
	if len(rep.Issues) != 1 {
		t.Fatalf("want exactly one issue for a short row, got %v", rep.Issues)
	}
	if rep.Errors != 1 || rep.Warnings != 0 {
		t.Fatalf("Errors=%d Warnings=%d, want 1/0 (%v)", rep.Errors, rep.Warnings, rep.Issues)
	}
	if !strings.Contains(rep.Issues[0].Message, "expected 6 fields") {
		t.Fatalf("unexpected issue: %v", rep.Issues[0])
	}
}

func TestRunFlagsDuplicateIDs(t *testing.T) {
	const data = `id;asset_name;ip;created_utc;source;category
1;a;1.2.3.4;27/02/2024 00:00;edr;phising
1;b;1.2.3.5;27/02/2024 00:00;edr;phising
`
	rep, err := Run(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Warnings != 1 || !strings.Contains(rep.Issues[0].Message, "duplicate id 1") {
		t.Fatalf("expected one duplicate-id warning, got %v", rep.Issues)
	}
	if rep.Issues[0].Line != 3 {
		t.Fatalf("duplicate reported on line %d, want 3", rep.Issues[0].Line)
	}
}

func TestRunWarnsOnlyOnUnmappableCategories(t *testing.T) {
	const data = `id;asset_name;ip;created_utc;source;category
1;a;1.2.3.4;27/02/2024 00:00;edr;phising
2;b;1.2.3.5;27/02/2024 00:00;edr;compromise (driveby)
3;c;1.2.3.6;27/02/2024 00:00;edr;valida_accounts
4;d;1.2.3.7;27/02/2024 00:00;edr;cryptomining
`
	rep, err := Run(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Warnings != 1 {
		t.Fatalf("Warnings=%d, want 1 (%v)", rep.Warnings, rep.Issues)
	}
	if rep.Issues[0].Line != 5 || !strings.Contains(rep.Issues[0].Message, "cryptomining") {
		t.Fatalf("wrong row flagged: %v", rep.Issues[0])
	}
}
