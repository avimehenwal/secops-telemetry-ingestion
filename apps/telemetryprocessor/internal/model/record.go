package model

type Record struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

type IngestRequest struct {
	Records []Record `json:"records"`
}

type Logline struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

type EnrichmentDetails struct {
	ASN           string `json:"asn"`
	Category      string `json:"category"`
	CorrelationID int64  `json:"correlationId"`
}

type AnalyticsEvent struct {
	ID            int64  `json:"id,omitempty"`
	Asset         string `json:"asset"`
	IP            string `json:"ip"`
	Category      string `json:"category"`
	ASN           string `json:"asn"`
	CorrelationID int64  `json:"correlationId"`
}

type AnalyticsSuccessResponse struct {
	Status        string `json:"status"`
	ItemsIngested int64  `json:"itemsIngested"`
}

type IngestResult struct {
	Received         int           `json:"received"`
	Enriched         int           `json:"enriched"`
	Ingested         int           `json:"ingested"`
	FailedEnrichment int           `json:"failedEnrichment"`
	FailedCategory   int           `json:"failedCategory"`
	FailedAnalytics  int           `json:"failedAnalytics"`
	RecordErrors     []RecordError `json:"recordErrors,omitempty"`

	DryRun bool `json:"dryRun,omitempty"`
}

type RecordError struct {
	ID     int64  `json:"id,omitempty"`
	Stage  string `json:"stage"` // "category" | "enrichment" | "analytics"
	Reason string `json:"reason"`
}
