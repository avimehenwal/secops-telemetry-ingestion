package category

import "testing"

// The canonical values are a contract with the Enrichment Service: anything
// this package produces is sent straight into its `category` enum, so a typo
// here becomes a 400 per record at runtime.
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

func TestNormalizeKnownAndMessy(t *testing.T) {
	cases := map[string]string{
		// Clean values, as the enum spells them.
		"phishing":                            Phishing,
		"content injection":                   ContentInjection,
		"trusted relationship":                TrustedRelationship,
		"supply chain compromise":             SupplyChainCompromise,
		"replication through removable media": ReplicationThroughRemovableMedia,
		"hardware additions":                  HardwareAdditions,

		// Separator and casing noise.
		"valid accounts":          ValidAccounts,
		"valid-accounts":          ValidAccounts,
		"supply_chain_compromise": SupplyChainCompromise,
		"content_injection":       ContentInjection,
		"trusted-relationship":    TrustedRelationship,
		"external-remote-service": ExternalRemoteServices,
		"external remote service": ExternalRemoteServices,
		"exploit public facing":   ExploitPublicFacingApplication,
		"drive by compromise":     DriveByCompromise,
		"Phising":                 Phishing,

		// Misspellings observed in docs/example_data_2.csv.
		"phising":                Phishing,
		"explaoit-public facing": ExploitPublicFacingApplication,
		"valida_accounts":        ValidAccounts,
		"compromise (driveby)":   DriveByCompromise,
	}
	for in, want := range cases {
		got, ok := Normalize(in)
		if !ok {
			t.Errorf("Normalize(%q): not recognised, want %q", in, want)
			continue
		}
		if got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

// Unknown values must be rejected rather than guessed at: forwarding one would
// earn a 400 from Enrichment, and silently dropping it would hide data loss.
func TestNormalizeUnknown(t *testing.T) {
	for _, in := range []string{"", "   ", "gibberish", "not a category", "cryptomining", "1234"} {
		if got, ok := Normalize(in); ok {
			t.Errorf("Normalize(%q) = %q, want not recognised", in, got)
		}
	}
}
