package ratelimit

import (
	"context"
	"sync"
	"time"
)

type clock struct {
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

func realSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Bucket struct {
	mu         sync.Mutex
	capacity   float64   // maximum tokens (== rate)
	refillRate float64   // tokens added per second
	tokens     float64   // current tokens
	last       time.Time // last time tokens were recomputed
	clk        clock
}

func New(rate int, window time.Duration) *Bucket {
	return newWithClock(rate, window, clock{now: time.Now, sleep: realSleep})
}

func newWithClock(rate int, window time.Duration, clk clock) *Bucket {
	return &Bucket{
		capacity:   float64(rate),
		refillRate: float64(rate) / window.Seconds(),
		tokens:     float64(rate),
		last:       clk.now(),
		clk:        clk,
	}
}

func (b *Bucket) Wait(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	need := float64(n)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		b.refill()
		if b.tokens >= need {
			b.tokens -= need
			b.mu.Unlock()
			return nil
		}
		deficit := need - b.tokens
		wait := time.Duration(deficit / b.refillRate * float64(time.Second))
		b.mu.Unlock()

		if err := b.clk.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func (b *Bucket) refill() {
	now := b.clk.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.refillRate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
}
