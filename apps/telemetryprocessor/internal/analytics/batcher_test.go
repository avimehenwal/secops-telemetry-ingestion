package analytics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

type recorder struct {
	mu    sync.Mutex
	sizes []int
	err   error
	n     int // items to report as ingested; -1 means "all of them"
}

func (r *recorder) send(_ context.Context, batch []model.AnalyticsEvent) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sizes = append(r.sizes, len(batch))
	if r.n >= 0 {
		return r.n, r.err
	}
	return len(batch), r.err
}

func (r *recorder) batches() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.sizes...)
}

func runBatcher(t *testing.T, rec *recorder, opts BatcherOptions) *Batcher {
	t.Helper()
	b := NewBatcher(rec.send, opts)
	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)
	t.Cleanup(func() {
		cancel()
		b.Wait()
	})
	return b
}

func TestBatcherCoalescesAcrossCallers(t *testing.T) {
	rec := &recorder{n: -1}
	b := runBatcher(t, rec, BatcherOptions{BatchSize: 20, MaxDelay: time.Hour})

	// Two "requests" of 10 each: one full batch, not two short ones.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results := make([]<-chan error, 0, 10)
			for j := 0; j < 10; j++ {
				results = append(results, b.Submit(context.Background(), model.AnalyticsEvent{ID: 1}))
			}
			for _, r := range results {
				if err := <-r; err != nil {
					t.Errorf("submit failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if got := rec.batches(); len(got) != 1 || got[0] != 20 {
		t.Fatalf("batches = %v, want exactly one batch of 20", got)
	}
}

func TestBatcherFlushesPartialBatchAfterMaxDelay(t *testing.T) {
	rec := &recorder{n: -1}
	b := runBatcher(t, rec, BatcherOptions{BatchSize: 20, MaxDelay: 20 * time.Millisecond})

	if err := <-b.Submit(context.Background(), model.AnalyticsEvent{ID: 7}); err != nil {
		t.Fatal(err)
	}
	if got := rec.batches(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("batches = %v, want one short batch of 1", got)
	}
}

func TestBatcherReportsSendFailureToEachWaiter(t *testing.T) {
	rec := &recorder{n: 0, err: errors.New("analytics down")}
	b := runBatcher(t, rec, BatcherOptions{BatchSize: 2, MaxDelay: 20 * time.Millisecond})

	r1 := b.Submit(context.Background(), model.AnalyticsEvent{ID: 1})
	r2 := b.Submit(context.Background(), model.AnalyticsEvent{ID: 2})
	if err := <-r1; err == nil {
		t.Error("first waiter did not see the failure")
	}
	if err := <-r2; err == nil {
		t.Error("second waiter did not see the failure")
	}
}

func TestBatcherAttributesPartialIngest(t *testing.T) {
	rec := &recorder{n: 1, err: errors.New("truncated")}
	b := runBatcher(t, rec, BatcherOptions{BatchSize: 2, MaxDelay: time.Hour})

	r1 := b.Submit(context.Background(), model.AnalyticsEvent{ID: 1})
	r2 := b.Submit(context.Background(), model.AnalyticsEvent{ID: 2})
	if err := <-r1; err != nil {
		t.Errorf("first record was ingested, got error %v", err)
	}
	if err := <-r2; err == nil {
		t.Error("second record was not ingested but reported success")
	}
}

// Shutdown must not strand a half-full batch.
func TestBatcherFlushesOnShutdown(t *testing.T) {
	rec := &recorder{n: -1}
	b := NewBatcher(rec.send, BatcherOptions{BatchSize: 20, MaxDelay: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)

	result := b.Submit(context.Background(), model.AnalyticsEvent{ID: 1})
	cancel()
	b.Wait()

	if err := <-result; err != nil {
		t.Fatalf("record dropped on shutdown: %v", err)
	}
	if got := rec.batches(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("batches = %v, want the pending record flushed", got)
	}
}

func TestBatcherDrainsQueueOnShutdown(t *testing.T) {
	rec := &recorder{n: -1}
	// BatchSize 2 with 5 records: one batch is pending, the rest are queued.
	b := NewBatcher(rec.send, BatcherOptions{BatchSize: 2, MaxDelay: time.Hour, Grace: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())

	results := make([]<-chan error, 0, 5)
	for i := 0; i < 5; i++ {
		results = append(results, b.Submit(context.Background(), model.AnalyticsEvent{ID: int64(i)}))
	}

	// Run only starts now, so every record is sitting in the queue.
	go b.Run(ctx)
	cancel()
	b.Wait()

	for i, r := range results {
		select {
		case err := <-r:
			if err != nil {
				t.Errorf("record %d failed on shutdown: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("record %d was dropped on shutdown", i)
		}
	}
	total := 0
	for _, n := range rec.batches() {
		total += n
	}
	if total != 5 {
		t.Errorf("delivered %d records across %v, want 5", total, rec.batches())
	}
}
