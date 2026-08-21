package model

type Record struct {
	ID       int64  `json:"id,omitempty"`
	Asset    string `json:"asset"`
	IP       string `json:"ip"`
	Category string `json:"category"`
}

func (r Record) Logline(normalisedCategory string) Logline {
	return Logline{ID: r.ID, Asset: r.Asset, IP: r.IP, Category: normalisedCategory}
}

func (r Record) AnalyticsEvent(details EnrichmentDetails) AnalyticsEvent {
	return AnalyticsEvent{
		ID:            r.ID,
		Asset:         r.Asset,
		IP:            r.IP,
		Category:      details.Category,
		ASN:           details.ASN,
		CorrelationID: details.CorrelationID,
	}
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

type Stage string

const (
	StageCategory   Stage = "category"   // free text we could not map to the enum
	StageEnrichment Stage = "enrichment" // the Enrichment Service would not enrich it
	StageAnalytics  Stage = "analytics"  // enriched, but it did not land upstream
)

type EventKind string

const (
	KindProgress EventKind = "progress"
	KindResult   EventKind = "result"
)

type IngestResult struct {
	Kind EventKind `json:"type,omitempty"`

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
	Stage  Stage  `json:"stage"`
	Reason string `json:"reason"`
}
