package enrichment

import (
	"sync"
	"time"
)

type breaker struct {
	mu       sync.Mutex
	trip     int
	cooldown time.Duration

	failures  int
	open      bool
	openUntil time.Time
	halfOpen  bool

	now func() time.Time // overridable in tests
}

func newBreaker(trip int, cooldown time.Duration) *breaker {
	return &breaker{trip: trip, cooldown: cooldown, now: time.Now}
}

func (b *breaker) allow() bool {
	if b.trip <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.open {
		return true
	}
	if b.now().Before(b.openUntil) {
		return false
	}
	b.open = false
	b.halfOpen = true
	return true
}

func (b *breaker) success() {
	if b.trip <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.halfOpen = false
	b.open = false
}

func (b *breaker) failure() {
	if b.trip <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.halfOpen {
		// Trial failed: straight back to open.
		b.halfOpen = false
		b.open = true
		b.openUntil = b.now().Add(b.cooldown)
		return
	}

	b.failures++
	if b.failures >= b.trip {
		b.open = true
		b.openUntil = b.now().Add(b.cooldown)
	}
}
