package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReadProgressStreamReportsEachEventAndReturnsResult(t *testing.T) {
	body := strings.NewReader(
		`{"type":"progress","received":45,"enriched":22,"ingested":20}` + "\n" +
			`{"type":"progress","received":45,"enriched":41,"ingested":40}` + "\n" +
			`{"type":"result","received":45,"enriched":45,"ingested":44,"failedAnalytics":1}` + "\n")

	var seen []int
	res, err := readProgressStream(context.Background(), body, func(ev ingestResult) {
		seen = append(seen, ev.Ingested)
	})
	if err != nil {
		t.Fatalf("readProgressStream: %v", err)
	}

	if len(seen) != 2 || seen[0] != 20 || seen[1] != 40 {
		t.Fatalf("progress callbacks saw %v, want [20 40]", seen)
	}
	if !res.accounted {
		t.Error("final result should be marked accounted")
	}
	if res.Ingested != 44 || res.FailedAnalytics != 1 {
		t.Fatalf("final result = %+v, want ingested 44 and 1 analytics failure", res)
	}
}

func TestReadProgressStreamTruncatedIsUnknownNotSuccess(t *testing.T) {
	body := strings.NewReader(
		`{"type":"progress","received":45,"ingested":20}` + "\n" +
			`{"type":"progress","received":45,"ing`) // cut mid-object

	_, err := readProgressStream(context.Background(), body, nil)
	if !errors.Is(err, errStreamTruncated) {
		t.Fatalf("err = %v, want errStreamTruncated", err)
	}
}

func TestReadProgressStreamWithoutResultIsTruncated(t *testing.T) {
	body := strings.NewReader(`{"type":"progress","received":45,"ingested":20}` + "\n")

	_, err := readProgressStream(context.Background(), body, nil)
	if !errors.Is(err, errStreamTruncated) {
		t.Fatalf("err = %v, want errStreamTruncated", err)
	}
}

func TestProgressLineUsesObservedRateForETA(t *testing.T) {
	// 20 of 100 records in 10s => 0.5s each => 40 remaining => ~20s to go.
	line := progressLine(time.Now().Add(-10*time.Second), 100, ingestResult{Ingested: 20, Enriched: 35})

	for _, want := range []string{"[ 20/100]", "20.0%", "enriched  35", "elapsed 00:10", "eta 00:40"} {
		if !strings.Contains(line, want) {
			t.Errorf("progress line %q missing %q", line, want)
		}
	}
}
