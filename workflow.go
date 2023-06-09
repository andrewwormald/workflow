package workflow

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/robfig/cron"
	"k8s.io/utils/clock"
)

type Workflow[T any] struct {
	Name string

	clock                   clock.Clock
	defaultPollingFrequency time.Duration
	defaultErrBackOff       time.Duration

	once sync.Once

	store  Store
	cursor Cursor

	processes map[string][]process[T]
	callback  map[string][]callback[T]
	timeouts  map[string]timeouts[T]

	graph     map[string][]string
	endPoints map[string]bool
}

func (w *Workflow[T]) Trigger(ctx context.Context, foreignID string, startingStatus string, opts ...TriggerOption[T]) (runID string, err error) {
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
		return "", err
	}

	hasExisitngWorkflow := true
	lastRunID, err := w.store.LastRunID(ctx, w.Name, foreignID)
	if errors.Is(err, ErrRunIDNotFound) {
		hasExisitngWorkflow = false
	} else if err != nil {
		return "", err
	}

	// Ensure the the previous existing workflow has been completed.
	if hasExisitngWorkflow {
		lastKey := MakeKey(w.Name, foreignID, lastRunID)
		lastRecord, err := w.store.LookupLatest(ctx, lastKey)
		if err != nil {
			return "", err
		}

		// Check that the last entry for that workflow was a terminal step when entered.
		if !lastRecord.IsEnd {
			// Cannot trigger a new workflow for this foreignID if there is a workflow in progress
			return "", errors.Wrap(ErrWorkflowInProgress, "")
		}
	}

	uid, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	key := MakeKey(w.Name, foreignID, uid.String())
	// isStart is always true when being stored as the trigger as it is the beginning of the workflow
	isStart := true

	// isEnd is always false as there should always be more than one node in the graph so that there can be a
	// transition between statuses / states.
	isEnd := false
	return key.RunID, w.store.Store(ctx, key, startingStatus, object, isStart, isEnd)
}

func (w *Workflow[T]) ScheduleTrigger(ctx context.Context, foreignID string, startingStatus string, spec string, opts ...TriggerOption[T]) error {
	schedule, err := cron.Parse(spec)
	if err != nil {
		return err
	}

	for {
		lastRunID, err := w.store.LastRunID(ctx, w.Name, foreignID)
		if errors.Is(err, ErrRunIDNotFound) {
			// NoReturnErr: Rather use zero value for lastRunID and use current clock for first run.
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "schedule trigger error - lookup latest record", j.MKV{
				"workflow_name":   w.Name,
				"foreignID":       foreignID,
				"starting_status": startingStatus,
			}))
			continue
		}

		var lastRun time.Time
		if lastRunID != "" {
			storeKey := MakeKey(w.Name, foreignID, lastRunID)
			latestEntry, err := w.store.LookupLatest(ctx, storeKey)
			if err != nil {
				if err != nil {
					log.Error(ctx, errors.Wrap(err, "schedule trigger error - lookup latest record", j.MKV{
						"workflow_name":   w.Name,
						"foreignID":       foreignID,
						"starting_status": startingStatus,
					}))
					continue
				}
			}

			// Use the last attempt as the last run
			lastRun = latestEntry.CreatedAt
		}

		// If there is no previous executions of this workflow then schedule the very next from now.
		if lastRun.IsZero() {
			lastRun = w.clock.Now()
		}

		nextRun := schedule.Next(lastRun)
		err = waitUntil(ctx, w.clock, nextRun)
		if err != nil {
			return err
		}

		_, err = w.Trigger(ctx, foreignID, startingStatus, opts...)
		if errors.Is(err, ErrWorkflowInProgress) {
			// NoReturnErr: Fallthrough to schedule next workflow as there is already one in progress. If this
			// happens it is likely that we scheduled a workflow and were unable to schedule the next.
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "schedule trigger error - triggering workflow", j.MKV{
				"workflow_name":   w.Name,
				"foreignID":       foreignID,
				"starting_status": startingStatus,
			}))
			continue
		}
	}
}

func waitUntil(ctx context.Context, clock clock.Clock, until time.Time) error {
	timeDiffAsDuration := until.Sub(clock.Now())

	t := clock.NewTimer(timeDiffAsDuration)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
		return nil
	}
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

func (w *Workflow[T]) Await(ctx context.Context, foreignID string, runID string, status string, opts ...AwaitOption) (*T, error) {
	var opt awaitOpts
	for _, option := range opts {
		option(&opt)
	}

	pollFrequency := w.defaultPollingFrequency
	if opt.pollFrequency.Nanoseconds() != 0 {
		pollFrequency = opt.pollFrequency
	}

	key := MakeKey(w.Name, foreignID, runID)
	return awaitWorkflowStatusByForeignID[T](ctx, w, key, status, pollFrequency)
}

type awaitOpts struct {
	pollFrequency time.Duration
}

type AwaitOption func(o *awaitOpts)

func WithPollingFrequency(d time.Duration) AwaitOption {
	return func(o *awaitOpts) {
		o.pollFrequency = d
	}
}

func (w *Workflow[T]) Callback(ctx context.Context, foreignID string, status string, payload io.Reader) error {
	for _, s := range w.callback[status] {
		err := processCallback(ctx, w, status, s.DestinationStatus, s.CallbackFunc, foreignID, payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Workflow[T]) Run(ctx context.Context) {
	// Ensure that the background consumers are only initialized once
	w.once.Do(func() {
		for currentStatus, processes := range w.processes {
			for _, p := range processes {
				if p.ParallelCount < 2 {
					// Launch all processes in runners
					go runner(ctx, w, currentStatus, p, 1, 1)
				} else {
					// Run as sharded parallel consumers
					for i := int64(0); i < p.ParallelCount; i++ {
						go runner(ctx, w, currentStatus, p, i, p.ParallelCount)
					}
				}
			}
		}

		for status, timeouts := range w.timeouts {
			go timeoutRunner(ctx, w, status, timeouts)
			go timeoutAutoInserter(ctx, w, status, timeouts)
		}
	})
}

func runner[T any](ctx context.Context, w *Workflow[T], currentStatus string, p process[T], shard, totalShards int64) {
	log.Info(ctx, "launched runner", j.MKV{
		"workflow_name": w.Name,
		"from":          currentStatus,
		"to":            p.DestinationStatus,
	})

	for {
		err := streamAndConsume(ctx, w, currentStatus, p, shard, totalShards)
		if errors.Is(err, ErrStreamingClosed) {
			log.Info(ctx, "shutting down process - runner", j.MKV{
				"workflow_name":      w.Name,
				"current_status":     currentStatus,
				"destination_status": p.DestinationStatus,
			})
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "runner error"))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute): // Incorporate a 1 - minute backoff
		}
	}
}

func timeoutRunner[T any](ctx context.Context, w *Workflow[T], currentStatus string, timeouts timeouts[T]) {
	log.Info(ctx, "launched timeout runner", j.MKV{
		"workflow_name": w.Name,
		"for":           currentStatus,
	})

	for {
		err := pollTimeouts(ctx, w, currentStatus, timeouts)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "runner error"))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(timeouts.ErrBackOff): // Incorporate a 1 - minute backoff
		}
	}
}

func timeoutAutoInserter[T any](ctx context.Context, w *Workflow[T], status string, timeouts timeouts[T]) {
	log.Info(ctx, "launched timeout auto inserter runner", j.MKV{
		"workflow_name": w.Name,
		"for":           status,
	})

	for {
		err := streamAndConsume(ctx, w, status, process[T]{
			PollingFrequency: timeouts.PollingFrequency,
			ErrBackOff:       timeouts.ErrBackOff,
			Consumer: func(ctx context.Context, key Key, t *T) (bool, error) {
				for _, config := range timeouts.Transitions {
					expireAt := w.clock.Now().Add(config.Duration)

					err := w.store.CreateTimeout(ctx, key, status, expireAt)
					if err != nil {
						return false, err
					}
				}

				// Never update state even when successful
				return false, nil
			},
			ParallelCount: 1,
		}, 1, 1)
		if errors.Is(err, ErrStreamingClosed) {
			log.Info(ctx, "shutting down process - timeout auto inserter", j.MKV{
				"workflow_name": w.Name,
				"status":        status,
			})
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "runner error"))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(timeouts.ErrBackOff):
		}
	}
}

type process[T any] struct {
	PollingFrequency  time.Duration
	ErrBackOff        time.Duration
	DestinationStatus string
	Consumer          ConsumerFunc[T]
	ParallelCount     int64
}

type callback[T any] struct {
	DestinationStatus string
	CallbackFunc      CallbackFunc[T]
}

type timeouts[T any] struct {
	PollingFrequency time.Duration
	ErrBackOff       time.Duration
	Transitions      []timeout[T]
}

type timeout[T any] struct {
	DestinationStatus string
	Duration          time.Duration
	TimeoutFunc       TimeoutFunc[T]
}

type ConsumerFunc[T any] func(ctx context.Context, key Key, t *T) (bool, error)

type CallbackFunc[T any] func(ctx context.Context, key Key, t *T, r io.Reader) (bool, error)

type TimeoutFunc[T any] func(ctx context.Context, key Key, t *T, now time.Time) (bool, error)

func Not[T any](c ConsumerFunc[T]) ConsumerFunc[T] {
	return func(ctx context.Context, key Key, t *T) (bool, error) {
		pass, err := c(ctx, key, t)
		if err != nil {
			return false, err
		}

		return !pass, nil
	}
}
