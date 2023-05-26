package workflow

import (
	"context"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"io"
	"time"

	"github.com/luno/jettison/errors"
)

func New[T any](name string, b Backends, opts ...Option[T]) *Workflow[T] {
	o := options[T]{
		processes: make(map[string][]Process[T]),
		callback: make(map[string][]struct {
			DestinationStatus Status
			CallbackFunc[T]
		}),
	}

	for _, opt := range opts {
		opt(&o)
	}

	return &Workflow[T]{
		Name:   name,
		store:  b.WorkflowStore(),
		cursor: b.WorkflowCursor(),
		opts:   o,
	}
}

var ErrRecordNotFound = errors.New("record not found")

type Store interface {
	LookupLatest(ctx context.Context, workflowName string, foreignID string) (*Record, error)
	Store(ctx context.Context, workflowName string, foreignID string, status string, object []byte) error
	Batch(ctx context.Context, workflowName string, status string, fromID int64, size int) ([]*Record, error)
	Find(ctx context.Context, workflowName string, foreignID string, status string) (*Record, error)
}

type Record struct {
	ID           int64
	WorkflowName string
	ForeignID    string
	Status       string
	Object       []byte
	CreatedAt    time.Time
}

type Backends interface {
	WorkflowStore() Store
	WorkflowCursor() Cursor
}

type Workflow[T any] struct {
	Name   string
	store  Store
	cursor Cursor
	opts   options[T]
}

func (w *Workflow[T]) Trigger(ctx context.Context, foreignID string, startingStatus Status, opts ...TriggerOption[T]) error {
	var o triggerOpts[T]
	for _, fn := range opts {
		fn(&o)
	}

	var t T
	if o.initialValue != nil {
		t = *o.initialValue
	}

	object, err := Marshal(&t)
	if err != nil {
		return err
	}

	return w.store.Store(ctx, w.Name, foreignID, startingStatus.String(), object)
}

type triggerOpts[T any] struct {
	initialValue   *T
	startingStatus int
}

type TriggerOption[T any] func(o *triggerOpts[T])

func WithInitialValue[T any](t *T) TriggerOption[T] {
	return func(o *triggerOpts[T]) {
		o.initialValue = t
	}
}

func (w *Workflow[T]) Await(ctx context.Context, foreignID string, status Status) (*T, error) {
	return awaitWorkflowStatusByForeignID[T](ctx, w.store, w.Name, foreignID, status.String())
}

func (w *Workflow[T]) Callback(ctx context.Context, foreignID string, status Status, payload io.Reader) error {
	for _, s := range w.opts.callback[status.String()] {
		err := processCallback(ctx, w, s.DestinationStatus.String(), s.CallbackFunc, foreignID, payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Workflow[T]) Run(ctx context.Context) {
	for currentStatus, processes := range w.opts.processes {
		for _, p := range processes {
			// Launch all processes in runners
			go runner(ctx, w, currentStatus, p)
		}
	}
}

func runner[T any](ctx context.Context, w *Workflow[T], currentStatus string, p Process[T]) {
	for {
		err := stream(ctx, w.cursor, w.store, w.Name, currentStatus, p)
		if errors.Is(err, ErrStreamingClosed) {
			log.Info(ctx, "shutting down process", j.MKV{
				"workflow_name":      w.Name,
				"current_status":     currentStatus,
				"destination_status": p.DestinationStatus,
			})
		}

		log.Error(ctx, errors.Wrap(err, "runner error"))
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute): // Incorporate a 1 minute backoff
		}
	}
}

type Option[T any] func(o *options[T])

type options[T any] struct {
	processes map[string][]Process[T]
	callback  map[string][]struct {
		DestinationStatus Status
		CallbackFunc[T]
	}

	startingPoint string
}

type Process[T any] struct {
	Consumer          Consumer[T]
	DestinationStatus Status
}

type Consumer[T any] func(*T) (bool, error)

type CallbackFunc[T any] func(t *T, r io.Reader) (bool, error)

func StatefulStep[T any](from Status, c Consumer[T], to Status) Option[T] {
	return func(o *options[T]) {
		o.processes[from.String()] = append(o.processes[from.String()], Process[T]{
			Consumer:          c,
			DestinationStatus: to,
		})
	}
}

func Callback[T any](from Status, fn CallbackFunc[T], to Status) Option[T] {
	return func(o *options[T]) {
		o.callback[from.String()] = append(o.callback[from.String()], struct {
			DestinationStatus Status
			CallbackFunc[T]
		}{
			DestinationStatus: to,
			CallbackFunc:      fn,
		})
	}
}
