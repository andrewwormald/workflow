package workflow

import (
	"context"
	"time"
)

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
// earlier on filtering process. True is returned when the event should be skipped.
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

func WithConsumerDeadLetterQueue(retryCount int) ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.RetryCount = retryCount
	}
}

func WithConsumerStreamFromLatest() ConsumerOption {
	return func(opt *ConsumerOptions) {
		opt.StreamFromLatest = true
	}
}
