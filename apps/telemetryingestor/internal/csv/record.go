package csv

import "strings"

const (
	ColID         = "id"
	ColAsset      = "asset_name"
	ColIP         = "ip"
	ColCreatedUTC = "created_utc"
	ColSource     = "source"
	ColCategory   = "category"
)

var Header = []string{ColID, ColAsset, ColIP, ColCreatedUTC, ColSource, ColCategory}

func Columns() []string { return append([]string(nil), Header...) }

type Record struct {
	Line       int
	ID         string
	Asset      string
	IP         string
	CreatedUTC string
	Source     string
	Category   string
}

func (r Record) Field(name string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case ColID:
		return r.ID, true
	case ColAsset:
		return r.Asset, true
	case ColIP:
		return r.IP, true
	case ColCreatedUTC:
		return r.CreatedUTC, true
	case ColSource:
		return r.Source, true
	case ColCategory:
		return r.Category, true
	default:
		return "", false
	}
}
