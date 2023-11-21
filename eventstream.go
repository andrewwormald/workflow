package workflow

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
)

type EventStreamer interface {
	NewProducer(topic string) Producer
	NewConsumer(topic string, name string, opts ...ConsumerOption) Consumer
}

type Producer interface {
	Send(ctx context.Context, e *Event) error
	Close() error
}

type Consumer interface {
	Recv(ctx context.Context) (*Event, Ack, error)
	Close() error
}

// Ack is used for the event streamer to update it's cursor of what messages have
// been consumed. If Ack is not called then the event streamer, depending on implementation,
// will likely not keep track of which records / events have been consumed.
type Ack func() error

type ConsumerOptions struct {
	PollFrequency    time.Duration
	LagAlert         time.Duration
	RetryCount       int
	StreamFromLatest bool
	EventFilter      EventFilter
}

// EventFilter can be passed to the event streaming implementation to allow specific consumers to have an
// earlier on filtering consumerConfig. True is returned when the event should be skipped.
type EventFilter func(e *Event) bool

type ConsumerOption func(*ConsumerOptions)

func WithEventFilter(ef EventFilter) ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.EventFilter = ef
	}
}

func WithConsumerPollFrequency(d time.Duration) ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.PollFrequency = d
	}
}

func WithConsumerLagAlert(d time.Duration) ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.LagAlert = d
	}
}

func WithConsumerDLQ(retryCount int) ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.RetryCount = retryCount
	}
}

func WithConsumerStreamFromLatest() ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.StreamFromLatest = true
	}
}

func awaitWorkflowStatusByForeignID[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, foreignID, runID string, role string, pollFrequency time.Duration) (*Record[Type, Status], error) {
	topic := Topic(w.Name, string(status))
	stream := w.eventStreamerFn.NewConsumer(
		topic,
		role,
		WithConsumerPollFrequency(pollFrequency),
		WithEventFilter(func(e *Event) bool {
			return e.ForeignID != foreignID
		}),
	)
	defer stream.Close()

	for {
		if ctx.Err() != nil {
			return nil, errors.Wrap(ErrWorkflowShutdown, "")
		}

		e, ack, err := stream.Recv(ctx)
		if err != nil {
			return nil, err
		}

		r, err := UnmarshalRecord(e.Body)
		if err != nil {
			return nil, err
		}

		switch true {
		// If the record doesn't match the status, foreignID, and runID then sleep and try again
		case r.Status != string(status), r.ForeignID != foreignID, r.RunID != runID:
			// Increment the offset / cursor to consumerConfig new events
			err = ack()
			if err != nil {
				return nil, err
			}

			continue
		}

		var t Type
		err = Unmarshal(r.Object, &t)
		if err != nil {
			return nil, err
		}

		return &Record[Type, Status]{
			WireRecord: *r,
			Status:     Status(r.Status),
			Object:     &t,
		}, ack()
	}
}
