package category

import "strings"

const (
	ContentInjection                 = "contentinjection"
	DriveByCompromise                = "drivebycompromise"
	ExploitPublicFacingApplication   = "exploitpublicfacingapplication"
	ExternalRemoteServices           = "externalremoteservices"
	HardwareAdditions                = "hardwareadditions"
	Phishing                         = "phishing"
	ReplicationThroughRemovableMedia = "replicationthroughremovablemedia"
	SupplyChainCompromise            = "supplychaincompromise"
	TrustedRelationship              = "trustedrelationship"
	ValidAccounts                    = "validaccounts"
)

var aliases = map[string]string{
	"contentinjection":                 ContentInjection,
	"drivebycompromise":                DriveByCompromise,
	"compromisedriveby":                DriveByCompromise,
	"exploitpublicfacingapplication":   ExploitPublicFacingApplication,
	"exploitpublicfacing":              ExploitPublicFacingApplication,
	"explaoitpublicfacing":             ExploitPublicFacingApplication,
	"externalremoteservices":           ExternalRemoteServices,
	"externalremoteservice":            ExternalRemoteServices,
	"hardwareadditions":                HardwareAdditions,
	"phishing":                         Phishing,
	"phising":                          Phishing,
	"replicationthroughremovablemedia": ReplicationThroughRemovableMedia,
	"supplychaincompromise":            SupplyChainCompromise,
	"trustedrelationship":              TrustedRelationship,
	"validaccounts":                    ValidAccounts,
	"validaaccounts":                   ValidAccounts,
}

func Normalize(raw string) (string, bool) {
	key := lettersOnly(raw)
	if key == "" {
		return "", false
	}
	canonical, ok := aliases[key]
	return canonical, ok
}

func lettersOnly(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		}
	}
	return b.String()
}
