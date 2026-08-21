package analytics

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/model"
)

const defaultMaxFillWait = 8 * time.Second

type Batcher struct {
	queue       chan submission
	batchSize   int
	maxFillWait time.Duration
	drainGrace  time.Duration
	sendBatch   func(context.Context, []model.AnalyticsEvent) (int, error)
	stopped     chan struct{}
	logger      *log.Logger
}

// submission is one event plus the channel its submitter is waiting on.
type submission struct {
	event   model.AnalyticsEvent
	outcome chan error
}

type BatcherOptions struct {
	BatchSize   int           // events per upstream request (clamped to UpstreamMaxBatchSize)
	MaxFillWait time.Duration // how long a partial batch waits for company
	QueueSize   int           // events buffered before Submit applies backpressure
	DrainGrace  time.Duration // budget for delivering what is queued at shutdown
	Logger      *log.Logger   // optional; each flush is logged here
}

func NewBatcher(sendBatch func(context.Context, []model.AnalyticsEvent) (int, error), opts BatcherOptions) *Batcher {
	if opts.BatchSize < 1 || opts.BatchSize > UpstreamMaxBatchSize {
		opts.BatchSize = UpstreamMaxBatchSize
	}
	if opts.MaxFillWait <= 0 {
		opts.MaxFillWait = defaultMaxFillWait
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = opts.BatchSize * 4
	}
	if opts.DrainGrace <= 0 {
		opts.DrainGrace = 30 * time.Second
	}
	return &Batcher{
		queue:       make(chan submission, opts.QueueSize),
		batchSize:   opts.BatchSize,
		maxFillWait: opts.MaxFillWait,
		drainGrace:  opts.DrainGrace,
		sendBatch:   sendBatch,
		stopped:     make(chan struct{}),
		logger:      opts.Logger,
	}
}

func (b *Batcher) logf(format string, args ...any) {
	if b.logger != nil {
		b.logger.Printf(format, args...)
	}
}

func (b *Batcher) Submit(ctx context.Context, event model.AnalyticsEvent) <-chan error {
	outcome := make(chan error, 1)
	select {
	case b.queue <- submission{event: event, outcome: outcome}:
	case <-ctx.Done():
		outcome <- ctx.Err()
		close(outcome)
	}
	return outcome
}

func (b *Batcher) Run(ctx context.Context) {
	defer close(b.stopped)

	batch := make([]submission, 0, b.batchSize)
	var fillDeadline <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			b.drain(ctx, batch)
			return

		case s := <-b.queue:
			batch = append(batch, s)
			if len(batch) < b.batchSize {
				if fillDeadline == nil {
					fillDeadline = time.After(b.maxFillWait)
				}
				continue
			}
			b.flush(ctx, batch)
			batch, fillDeadline = batch[:0], nil

		case <-fillDeadline:
			// Nobody else is coming: send a short batch rather than strand it.
			b.flush(ctx, batch)
			batch, fillDeadline = batch[:0], nil
		}
	}
}

func (b *Batcher) Wait() { <-b.stopped }

func (b *Batcher) drain(ctx context.Context, batch []submission) {
	for drained := false; !drained; {
		select {
		case s := <-b.queue:
			batch = append(batch, s)
		default:
			drained = true
		}
	}

	graceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.drainGrace)
	defer cancel()

	for len(batch) > b.batchSize {
		b.flush(graceCtx, batch[:b.batchSize])
		batch = batch[b.batchSize:]
	}
	b.flush(graceCtx, batch)
}

func (b *Batcher) flush(ctx context.Context, batch []submission) {
	if len(batch) == 0 {
		return
	}

	events := make([]model.AnalyticsEvent, len(batch))
	for i, s := range batch {
		events[i] = s.event
	}

	start := time.Now()
	ingested, err := b.sendBatch(ctx, events)
	if err != nil {
		b.logf("analytics batch sent: size=%d ingested=%d duration=%s error=%v", len(batch), ingested, time.Since(start), err)
	} else {
		b.logf("analytics batch sent: size=%d ingested=%d duration=%s", len(batch), ingested, time.Since(start))
	}

	if err == nil && ingested < len(batch) {
		err = fmt.Errorf("analytics accepted only %d of %d events", ingested, len(batch))
	}

	for i, s := range batch {
		if err != nil && i >= ingested {
			s.outcome <- err
		} else {
			s.outcome <- nil
		}
		close(s.outcome)
	}
}
