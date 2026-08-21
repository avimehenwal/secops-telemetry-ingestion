package enrichment

import (
	"sync"
	"time"
)

const maxFailedTrials = 2

type breaker struct {
	mu            sync.Mutex
	tripThreshold int
	cooldown      time.Duration

	consecutiveFailures int
	open                bool
	openUntil           time.Time
	halfOpen            bool
	failedTrials        int // consecutive half-open trials that failed

	now func() time.Time // overridable in tests
}

func newBreaker(tripThreshold int, cooldown time.Duration) *breaker {
	return &breaker{tripThreshold: tripThreshold, cooldown: cooldown, now: time.Now}
}

func (b *breaker) allow() (ok bool, retryIn time.Duration, hardDown bool) {
	if b.tripThreshold <= 0 {
		return true, 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.open {
		return true, 0, false
	}
	if remaining := b.openUntil.Sub(b.now()); remaining > 0 {
		return false, remaining, b.failedTrials >= maxFailedTrials
	}
	b.open = false
	b.halfOpen = true
	return true, 0, false
}

func (b *breaker) success() {
	if b.tripThreshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailures = 0
	b.failedTrials = 0
	b.halfOpen = false
	b.open = false
}

func (b *breaker) failure() {
	if b.tripThreshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.halfOpen {
		// Trial failed: straight back to open.
		b.halfOpen = false
		b.open = true
		b.failedTrials++
		b.openUntil = b.now().Add(b.cooldown)
		return
	}

	b.consecutiveFailures++
	if b.consecutiveFailures >= b.tripThreshold {
		b.open = true
		b.openUntil = b.now().Add(b.cooldown)
	}
}
