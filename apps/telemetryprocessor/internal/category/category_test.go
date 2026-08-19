package category

import "testing"

func TestNormalizeKnownAndMessy(t *testing.T) {
	cases := map[string]string{
		// clean
		"phishing":                            Phishing,
		"content injection":                   ContentInjection,
		"trusted relationship":                TrustedRelationship,
		"supply chain compromise":             SupplyChainCompromise,
		"replication through removable media": ReplicationThroughRemovableMedia,
		// messy / observed in the sample data
		"phising":                             Phishing,
		"Phising":                             Phishing,
		"valida_accounts":                     ValidAccounts,
		"valid accounts":                      ValidAccounts,
		"valid-accounts":                      ValidAccounts,
		"explaoit-public facing":              ExploitPublicFacingApplication,
		"exploit public facing":               ExploitPublicFacingApplication,
		"external-remote-service":             ExternalRemoteServices,
		"external remote service":             ExternalRemoteServices,
		"compromise (driveby)":                DriveByCompromise,
		"drive-by-compromise":                 DriveByCompromise,
		"drive by compromise":                 DriveByCompromise,
		"supply_chain_compromise":             SupplyChainCompromise,
		"content_injection":                   ContentInjection,
		"trusted-relationship":                TrustedRelationship,
		"Replication through removable media": ReplicationThroughRemovableMedia,
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

func TestNormalizeUnknown(t *testing.T) {
	for _, in := range []string{"", "   ", "gibberish", "not a category"} {
		if got, ok := Normalize(in); ok {
			t.Errorf("Normalize(%q) = %q, want not recognised", in, got)
		}
	}
}
