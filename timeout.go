package workflow

import (
	"context"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"time"
)

type Timeout struct {
	ID           int64
	WorkflowName string
	ForeignID    string
	RunID        string
	Status       string
	Completed    bool
	ExpireAt     time.Time
	CreatedAt    time.Time
}

// pollTimeouts attempts to find the very next
func pollTimeouts[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrWorkflowShutdown, "")
		}

		expiredTimeouts, err := w.timeoutStore.ListValid(ctx, w.Name, string(status), w.clock.Now())
		if err != nil {
			return err
		}

		for _, expiredTimeout := range expiredTimeouts {
			r, err := w.recordStore.Latest(ctx, expiredTimeout.WorkflowName, expiredTimeout.ForeignID)
			if err != nil {
				return err
			}

			if r.Status != string(status) {
				// Object has been updated already. Mark timeout as cancelled as it is no longer valid.
				err = w.timeoutStore.Cancel(ctx, expiredTimeout.WorkflowName, expiredTimeout.ForeignID, expiredTimeout.RunID, expiredTimeout.Status)
				if err != nil {
					return err
				}
			}

			var t Type
			err = Unmarshal(r.Object, &t)
			if err != nil {
				return err
			}

			record := Record[Type, Status]{
				WireRecord: *r,
				Status:     Status(r.Status),
				Object:     &t,
			}

			for _, config := range timeouts.Transitions {
				ok, err := config.TimeoutFunc(ctx, &record, w.clock.Now())
				if err != nil {
					return err
				}

				if ok {
					object, err := Marshal(&t)
					if err != nil {
						return err
					}

					wr := &WireRecord{
						WorkflowName: record.WorkflowName,
						ForeignID:    record.ForeignID,
						RunID:        record.RunID,
						Status:       string(config.DestinationStatus),
						IsStart:      false,
						IsEnd:        w.endPoints[config.DestinationStatus],
						Object:       object,
						CreatedAt:    record.CreatedAt,
					}

					err = update(ctx, w.eventStreamerFn, w.recordStore, wr)
					if err != nil {
						return err
					}

					// Mark timeout as having been executed (aka completed) only in the case that true is returned.
					err = w.timeoutStore.Complete(ctx, record.WorkflowName, record.ForeignID, record.RunID, string(record.Status))
					if err != nil {
						return err
					}
				}
			}
		}

		err = wait(ctx, timeouts.PollingFrequency)
		if err != nil {
			return err
		}
	}
}

type timeouts[Type any, Status ~string] struct {
	PollingFrequency time.Duration
	ErrBackOff       time.Duration
	Transitions      []timeout[Type, Status]
}

type timeout[Type any, Status ~string] struct {
	DestinationStatus Status
	TimerFunc         TimerFunc[Type, Status]
	TimeoutFunc       TimeoutFunc[Type, Status]
}

func timeoutPoller[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) {
	if w.debugMode {
		log.Info(ctx, "launched timeout consumer", j.MKV{
			"workflow_name": w.Name,
			"for":           status,
		})
	}

	role := makeRole(w.Name, string(status), "timeout-consumer")

	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter consumer error"))
		}

		w.updateState(role, StateRunning)

		err = pollTimeouts(ctx, w, status, timeouts)
		if errors.IsAny(err, ErrWorkflowShutdown, context.Canceled) {
			cancel()
			return
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout consumer error"))
		}

		select {
		case <-ctx.Done():
			cancel()
			return
		case <-time.After(timeouts.ErrBackOff):
			cancel()
		}
	}
}

func timeoutAutoInserterConsumer[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) {
	if w.debugMode {
		log.Info(ctx, "launched timeout auto inserter consumer", j.MKV{
			"workflow_name": w.Name,
			"for":           status,
		})
	}

	role := makeRole(w.Name, string(status), "timeout-auto-inserter-consumer")

	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter consumer error"))
		}

		w.updateState(role, StateRunning)

		cunsumerFunc := func(ctx context.Context, r *Record[Type, Status]) (bool, error) {
			for _, config := range timeouts.Transitions {
				ok, expireAt, err := config.TimerFunc(ctx, r, w.clock.Now())
				if err != nil {
					return false, err
				}

				if !ok {
					// Ignore and evaluate the next
					continue
				}

				err = w.timeoutStore.Create(ctx, r.WorkflowName, r.ForeignID, r.RunID, string(status), expireAt)
				if err != nil {
					return false, err
				}
			}

			// Never update State even when successful
			return false, nil
		}

		err = runStepConsumerForever(ctx, w, consumerConfig[Type, Status]{
			PollingFrequency: timeouts.PollingFrequency,
			ErrBackOff:       timeouts.ErrBackOff,
			Consumer:         cunsumerFunc,
			ParallelCount:    1,
		},
			status,
			role,
			1,
			1,
		)
		if errors.IsAny(err, ErrWorkflowShutdown, context.Canceled) {
			if w.debugMode {
				log.Info(ctx, "shutting down consumerConfig - timeout auto inserter consumer", j.MKV{
					"workflow_name": w.Name,
					"status":        status,
				})
			}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter consumer error"))
		}

		select {
		case <-ctx.Done():
			cancel()
			return
		case <-time.After(timeouts.ErrBackOff):
			cancel()
		}
	}
}

type TimerFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, time.Time, error)

type TimeoutFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, error)
