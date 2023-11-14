package workflow

import (
	"context"
	"fmt"
	"github.com/robfig/cron/v3"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"k8s.io/utils/clock"
)

type Workflow[Type any, Status ~string] struct {
	Name string

	clock                   clock.Clock
	defaultPollingFrequency time.Duration
	defaultErrBackOff       time.Duration

	calledRun bool
	once      sync.Once

	eventStreamerFn EventStreamerConstructor
	recordStore     RecordStore
	timeoutStore    TimeoutStore
	scheduler       RoleScheduler

	processes map[Status][]process[Type, Status]
	callback  map[Status][]callback[Type, Status]
	timeouts  map[Status]timeouts[Type, Status]

	graph         map[Status][]Status
	endPoints     map[Status]bool
	validStatuses map[Status]bool

	debugMode bool
}

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

func (w *Workflow[Type, Status]) ScheduleTrigger(ctx context.Context, foreignID string, startingStatus Status, spec string, opts ...TriggerOption[Type, Status]) error {
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

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter runner error"))
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

func (w *Workflow[Type, Status]) Await(ctx context.Context, foreignID, runID string, status Status, opts ...AwaitOption) (*Record[Type, Status], error) {
	var opt awaitOpts
	for _, option := range opts {
		option(&opt)
	}

	pollFrequency := w.defaultPollingFrequency
	if opt.pollFrequency.Nanoseconds() != 0 {
		pollFrequency = opt.pollFrequency
	}

	role := makeRole("await", w.Name, string(status), foreignID, runID)
	return awaitWorkflowStatusByForeignID[Type, Status](ctx, w, status, foreignID, runID, role, pollFrequency)
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

func (w *Workflow[Type, Status]) Callback(ctx context.Context, foreignID string, status Status, payload io.Reader) error {
	for _, s := range w.callback[status] {
		err := processCallback(ctx, w, status, s.DestinationStatus, s.CallbackFunc, foreignID, payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Workflow[Type, Status]) Run(ctx context.Context) {
	// Ensure that the background consumers are only initialized once
	w.once.Do(func() {
		w.calledRun = true

		for currentStatus, processes := range w.processes {
			for _, p := range processes {
				go runner(ctx, w, currentStatus, p)
			}
		}

		for status, timeouts := range w.timeouts {
			go timeoutRunner(ctx, w, status, timeouts)
			go timeoutAutoInserterRunner(ctx, w, status, timeouts)
		}
	})
}

func runner[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], currentStatus Status, p process[Type, Status]) {
	if w.debugMode {
		log.Info(ctx, "launched runner", j.MKV{
			"workflow_name": w.Name,
			"from":          currentStatus,
			"to":            p.DestinationStatus,
		})
	}

	role := makeRole(
		w.Name,
		string(currentStatus),
		"to",
		string(p.DestinationStatus),
		"consumer",
	)

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "runner error"))
		}

		if w.debugMode {
			log.Info(ctx, "runner obtained role", j.MKV{
				"role": role,
			})
		}

		if ctx.Err() != nil {
			// Gracefully exit when context has been cancelled
			if w.debugMode {
				log.Info(ctx, "shutting down process - runner", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
					"role":               role,
				})
			}
			return
		}

		err = runStepConsumerForever[Type, Status](ctx, w, p, currentStatus, role)
		if errors.Is(err, ErrStreamingClosed) {
			if w.debugMode {
				log.Info(ctx, "shutting down process - runner", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
				})
			}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "runner error"))
		}

		select {
		case <-ctx.Done():
			cancel()
			if w.debugMode {
				log.Info(ctx, "shutting down process - runner", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
				})
			}
			return
		case <-time.After(p.ErrBackOff):
			cancel()
		}
	}
}

func timeoutRunner[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) {
	if w.debugMode {
		log.Info(ctx, "launched timeout runner", j.MKV{
			"workflow_name": w.Name,
			"for":           status,
		})
	}

	role := makeRole(w.Name, string(status), "timeout-runner")

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter runner error"))
		}

		err = pollTimeouts(ctx, w, status, timeouts)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout runner error"))
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

func timeoutAutoInserterRunner[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) {
	if w.debugMode {
		log.Info(ctx, "launched timeout auto inserter runner", j.MKV{
			"workflow_name": w.Name,
			"for":           status,
		})
	}

	role := makeRole(w.Name, string(status), "timeout-auto-inserter-runner")

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter runner error"))
		}

		err = runStepConsumerForever(ctx, w, process[Type, Status]{
			PollingFrequency: timeouts.PollingFrequency,
			ErrBackOff:       timeouts.ErrBackOff,
			Consumer: func(ctx context.Context, r *Record[Type, Status]) (bool, error) {
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

				// Never update state even when successful
				return false, nil
			},
			ParallelCount: 1,
		},
			status,
			role,
		)
		if errors.Is(err, ErrStreamingClosed) {
			if w.debugMode {
				log.Info(ctx, "shutting down process - timeout auto inserter runner", j.MKV{
					"workflow_name": w.Name,
					"status":        status,
				})
			}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "timeout auto inserter runner error"))
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

func makeRole(inputs ...string) string {
	joined := strings.Join(inputs, "-")
	lowered := strings.ToLower(joined)
	filled := strings.Replace(lowered, " ", "_", -1)
	return filled
}

type process[Type any, Status ~string] struct {
	PollingFrequency  time.Duration
	ErrBackOff        time.Duration
	DestinationStatus Status
	Consumer          ConsumerFunc[Type, Status]
	ParallelCount     int64
}

type callback[Type any, Status ~string] struct {
	DestinationStatus Status
	CallbackFunc      CallbackFunc[Type, Status]
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

type ConsumerFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status]) (bool, error)

type CallbackFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], reader io.Reader) (bool, error)

type TimerFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, time.Time, error)

type TimeoutFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, error)

func Not[Type any, Status ~string](c ConsumerFunc[Type, Status]) ConsumerFunc[Type, Status] {
	return func(ctx context.Context, r *Record[Type, Status]) (bool, error) {
		pass, err := c(ctx, r)
		if err != nil {
			return false, err
		}

		return !pass, nil
	}
}

func DurationTimerFunc[Type any, Status ~string](duration time.Duration) TimerFunc[Type, Status] {
	return func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, time.Time, error) {
		return true, now.Add(duration), nil
	}
}

func TimeTimerFunc[Type any, Status ~string](t time.Time) TimerFunc[Type, Status] {
	return func(ctx context.Context, r *Record[Type, Status], now time.Time) (bool, time.Time, error) {
		return true, t, nil
	}
}
