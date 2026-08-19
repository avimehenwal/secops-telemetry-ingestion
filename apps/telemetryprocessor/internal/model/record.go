// Package model contains the data shapes shared across the telemetry
// processor: the inbound record from the CLI, and the shapes exchanged with
// the Enrichment and Analytics services (see docs/openapi.json).
package model

// Record is a single security log line as ingested from the CLI.
type Record struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

// IngestRequest is the payload the CLI sends to the processor's single
// endpoint.
type IngestRequest struct {
	Records []Record `json:"records"`
}

// EnrichmentDetails is the response returned by the Enrichment Service for
// a single record.
type EnrichmentDetails struct {
	ASN           string `json:"asn"`
	Category      string `json:"category"`
	CorrelationID int64  `json:"correlationId"`
}

// AnalyticsEvent is a record enriched and ready to be sent to the
// Analytics Service. The Analytics Service accepts batches of up to 20
// events per request.
type AnalyticsEvent struct {
	ID            int64  `json:"id,omitempty"`
	Asset         string `json:"asset"`
	IP            string `json:"ip"`
	Category      string `json:"category"`
	ASN           string `json:"asn"`
	CorrelationID int64  `json:"correlationId"`
}
