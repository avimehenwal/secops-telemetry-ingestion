package analytics

import (
	"context"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

type Batcher struct {
	in        chan submission
	batchSize int
	maxDelay  time.Duration
	grace     time.Duration
	send      func(context.Context, []model.AnalyticsEvent) (int, error)
	done      chan struct{}
}

type submission struct {
	event  model.AnalyticsEvent
	result chan error
}

type BatcherOptions struct {
	BatchSize int           // events per upstream request (clamped to UpstreamMaxBatchSize)
	MaxDelay  time.Duration // how long a partial batch waits for company
	QueueSize int           // events buffered before Submit applies backpressure
	Grace     time.Duration // budget for delivering what is queued at shutdown
}

func NewBatcher(send func(context.Context, []model.AnalyticsEvent) (int, error), opts BatcherOptions) *Batcher {
	if opts.BatchSize < 1 || opts.BatchSize > UpstreamMaxBatchSize {
		opts.BatchSize = UpstreamMaxBatchSize
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 2 * time.Second
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = opts.BatchSize * 4
	}
	if opts.Grace <= 0 {
		opts.Grace = 30 * time.Second
	}
	return &Batcher{
		in:        make(chan submission, opts.QueueSize),
		batchSize: opts.BatchSize,
		maxDelay:  opts.MaxDelay,
		grace:     opts.Grace,
		send:      send,
		done:      make(chan struct{}),
	}
}

func (b *Batcher) Submit(ctx context.Context, ev model.AnalyticsEvent) <-chan error {
	result := make(chan error, 1)
	select {
	case b.in <- submission{event: ev, result: result}:
	case <-ctx.Done():
		result <- ctx.Err()
		close(result)
	}
	return result
}

func (b *Batcher) Run(ctx context.Context) {
	defer close(b.done)

	pending := make([]submission, 0, b.batchSize)
	var idle <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			b.shutdown(ctx, pending)
			return

		case s := <-b.in:
			pending = append(pending, s)
			if len(pending) < b.batchSize {
				if idle == nil {
					idle = time.After(b.maxDelay)
				}
				continue
			}
			b.flush(ctx, pending)
			pending, idle = pending[:0], nil

		case <-idle:
			// Nobody else is coming: send a short batch rather than strand it.
			b.flush(ctx, pending)
			pending, idle = pending[:0], nil
		}
	}
}

func (b *Batcher) Wait() { <-b.done }

func (b *Batcher) shutdown(ctx context.Context, pending []submission) {
	for drained := false; !drained; {
		select {
		case s := <-b.in:
			pending = append(pending, s)
		default:
			drained = true
		}
	}

	graceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.grace)
	defer cancel()

	for len(pending) > b.batchSize {
		b.flush(graceCtx, pending[:b.batchSize])
		pending = pending[b.batchSize:]
	}
	b.flush(graceCtx, pending)
}

func (b *Batcher) flush(ctx context.Context, pending []submission) {
	if len(pending) == 0 {
		return
	}

	events := make([]model.AnalyticsEvent, len(pending))
	for i, s := range pending {
		events[i] = s.event
	}

	ingested, err := b.send(ctx, events)

	for i, s := range pending {
		if err != nil && i >= ingested {
			s.result <- err
		} else {
			s.result <- nil
		}
		close(s.result)
	}
}
