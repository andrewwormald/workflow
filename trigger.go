package workflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/robfig/cron/v3"
	"k8s.io/utils/clock"
)

func (w *Workflow[Type, Status]) Trigger(ctx context.Context, foreignID string, startingStatus Status, opts ...TriggerOption[Type, Status]) (runID string, err error) {
	if !w.calledRun {
		return "", errors.Wrap(ErrWorkflowNotRunning, "ensure Run() is called before attempting to trigger the workflow")
	}

	_, ok := w.validStatuses[startingStatus]
	if !ok {
		return "", errors.Wrap(ErrStatusProvidedNotConfigured, fmt.Sprintf("ensure %v is configured for workflow: %v", startingStatus, w.Name))
	}

	var o triggerOpts[Type, Status]
	for _, fn := range opts {
		fn(&o)
	}

	var t Type
	if o.initialValue != nil {
		t = *o.initialValue
	}

	object, err := Marshal(&t)
	if err != nil {
		return "", err
	}

	lastRecord, err := w.recordStore.Latest(ctx, w.Name, foreignID)
	if errors.Is(err, ErrRecordNotFound) {
		lastRecord = &WireRecord{}
	} else if err != nil {
		return "", err
	}

	// Check that the last entry for that workflow was a terminal step when entered.
	if lastRecord.RunID != "" && !lastRecord.IsEnd {
		// Cannot trigger a new workflow for this foreignID if there is a workflow in progress
		return "", errors.Wrap(ErrWorkflowInProgress, "")
	}

	uid, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	runID = uid.String()
	wr := &WireRecord{
		RunID:        runID,
		WorkflowName: w.Name,
		ForeignID:    foreignID,
		Status:       string(startingStatus),
		// isStart is always true when being stored as the trigger as it is the beginning of the workflow
		IsStart: true,
		// isEnd is always false as there should always be more than one node in the graph so that there can be a
		// transition between statuses / states.
		IsEnd:     false,
		Object:    object,
		CreatedAt: w.clock.Now(),
	}

	err = update(ctx, w.eventStreamerFn, w.recordStore, wr)
	if err != nil {
		return "", err
	}

	return runID, nil
}

func (w *Workflow[Type, Status]) ScheduleTrigger(foreignID string, startingStatus Status, spec string, opts ...TriggerOption[Type, Status]) error {
	if !w.calledRun {
		return errors.Wrap(ErrWorkflowNotRunning, "ensure Run() is called before attempting to trigger the workflow")
	}

	_, ok := w.validStatuses[startingStatus]
	if !ok {
		return errors.Wrap(ErrStatusProvidedNotConfigured, fmt.Sprintf("ensure %v is configured for workflow: %v", startingStatus, w.Name))
	}

	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	role := strings.Join([]string{w.Name, string(startingStatus), foreignID, "scheduler", spec}, "-")

	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	for {
		ctx, cancel, err := w.scheduler.Await(w.ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "schedule trigger error - awaiting role"))
			continue
		}

		w.updateState(role, StateRunning)

		if ctx.Err() != nil {
			// Gracefully exit when context has been cancelled
			if w.debugMode {
				log.Info(ctx, "shutting down scheduled trigger", j.MKV{
					"workflow_name":   w.Name,
					"starting_status": startingStatus,
					"role":            role,
				})
			}
			return nil
		}

		latestEntry, err := w.recordStore.Latest(ctx, w.Name, foreignID)
		if errors.Is(err, ErrRecordNotFound) {
			// NoReturnErr: Rather use zero value for lastRunID and use current clock for first run.
			latestEntry = &WireRecord{}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "schedule trigger error - lookup latest record", j.MKV{
				"workflow_name":   w.Name,
				"foreignID":       foreignID,
				"starting_status": startingStatus,
			}))
			cancel()
			continue
		}

		lastRun := latestEntry.CreatedAt

		// If there is no previous executions of this workflow then schedule the very next from now.
		if lastRun.IsZero() {
			lastRun = w.clock.Now()
		}

		nextRun := schedule.Next(lastRun)
		err = waitUntil(ctx, w.clock, nextRun)
		if errors.Is(err, context.Canceled) {
			cancel()
			continue
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "schedule trigger error - wait until", j.MKV{
				"workflow_name": w.Name,
				"now":           w.clock.Now(),
				"spec":          spec,
				"last_run":      lastRun,
				"next_run":      nextRun,
			}))
			cancel()
			continue
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
			cancel()
			continue
		}

		// Always cancel the context to release the role
		cancel()
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

type triggerOpts[Type any, Status ~string] struct {
	initialValue   *Type
	startingStatus int
}

type TriggerOption[Type any, Status ~string] func(o *triggerOpts[Type, Status])

func WithInitialValue[Type any, Status ~string](t *Type) TriggerOption[Type, Status] {
	return func(o *triggerOpts[Type, Status]) {
		o.initialValue = t
	}
}
