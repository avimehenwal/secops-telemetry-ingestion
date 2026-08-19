package category

import "testing"

func TestCanonicalNamesMatchOpenAPIEnum(t *testing.T) {
	want := []string{
		"contentinjection", "drivebycompromise", "exploitpublicfacingapplication",
		"externalremoteservices", "hardwareadditions", "phishing",
		"replicationthroughremovablemedia", "supplychaincompromise",
		"trustedrelationship", "validaccounts",
	}
	canonical := make(map[string]bool)
	for _, v := range aliases {
		canonical[v] = true
	}
	for _, w := range want {
		if !canonical[w] {
			t.Errorf("no alias maps to canonical category %q", w)
		}
		delete(canonical, w)
	}
	for extra := range canonical {
		t.Errorf("alias table produces %q, which is not in the OpenAPI enum", extra)
	}
}

func TestNormalizeHandlesDatasetSpellings(t *testing.T) {
	tests := map[string]string{
		"phising":                             Phishing,
		"Phising":                             Phishing,
		"compromise (driveby)":                DriveByCompromise,
		"drive-by-compromise":                 DriveByCompromise,
		"explaoit-public facing":              ExploitPublicFacingApplication,
		"valida_accounts":                     ValidAccounts,
		"Replication through removable media": ReplicationThroughRemovableMedia,
	}
	for in, want := range tests {
		got, ok := Normalize(in)
		if !ok || got != want {
			t.Errorf("Normalize(%q) = %q,%v; want %q,true", in, got, ok, want)
		}
	}
	if _, ok := Normalize("cryptomining"); ok {
		t.Error("Normalize accepted an unknown category")
	}
}
