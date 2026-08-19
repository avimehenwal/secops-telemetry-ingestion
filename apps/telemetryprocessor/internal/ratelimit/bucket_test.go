package ratelimit

import (
	"context"
	"testing"
	"time"
)

// fakeClock advances only when sleep is called, making Wait deterministic.
type fakeClock struct {
	t time.Time
}

func (f *fakeClock) now() time.Time { return f.t }
func (f *fakeClock) sleep(_ context.Context, d time.Duration) error {
	f.t = f.t.Add(d)
	return nil
}

func newFakeBucket(rate int, window time.Duration) (*Bucket, *fakeClock) {
	fc := &fakeClock{t: time.Unix(0, 0)}
	b := newWithClock(rate, window, clock{now: fc.now, sleep: fc.sleep})
	return b, fc
}

func TestBucketInitialBurst(t *testing.T) {
	b, fc := newFakeBucket(20, 10*time.Second)
	// The full initial bucket should let 20 messages through with no waiting.
	if err := b.Wait(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	if fc.t != time.Unix(0, 0) {
		t.Fatalf("initial burst should not sleep, clock advanced to %v", fc.t)
	}
}

func TestBucketThrottlesAfterBurst(t *testing.T) {
	b, fc := newFakeBucket(20, 10*time.Second)
	_ = b.Wait(context.Background(), 20) // drain

	// The next message must wait for a token to refill: 20 tokens / 10s =
	// 2 tokens/s, so one token takes 0.5s.
	start := fc.t
	if err := b.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waited := fc.t.Sub(start)
	if waited < 490*time.Millisecond || waited > 520*time.Millisecond {
		t.Fatalf("expected ~500ms wait for 1 token, got %v", waited)
	}
}

func TestBucketRespectsContextCancellation(t *testing.T) {
	b, _ := newFakeBucket(1, time.Hour) // very slow refill
	_ = b.Wait(context.Background(), 1) // drain

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx, 1); err == nil {
		t.Fatal("expected context cancellation error")
	}
}
