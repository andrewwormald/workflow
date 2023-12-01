package workflow

import (
	"context"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"io"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type API[Type any, Status ~string] interface {
	// Trigger will kickstart a workflow for the provided foreignID starting from the provided starting status. There
	// is no limitation as to where you start the workflow from. For workflows that have data preceding the initial
	// trigger that needs to be used in the workflow, using WithInitialValue will allow you to provide pre-populated
	// fields of Type that can be accessed by the consumers.
	//
	// foreignID should not be random and should be deterministic for the thing that you are running the workflow for.
	// This especially helps when connecting other workflows as the foreignID is the only way to connect the streams. The
	// same goes for Callback as you will need the foreignID to connect the callback back to the workflow instance that
	// was run.
	Trigger(ctx context.Context, foreignID string, startingStatus Status, opts ...TriggerOption[Type, Status]) (runID string, err error)

	// ScheduleTrigger takes a cron spec and will call Trigger at the specified intervals. ScheduleTrigger is a blocking
	// call and will return ErrWorkflowNotRunning or ErrStatusProvidedNotConfigured to indicate that it cannot begin to
	// schedule. All schedule errors will be retried indefinitely. The same options are available for ScheduleTrigger
	// as they are for Trigger.
	ScheduleTrigger(ctx context.Context, foreignID string, startingStatus Status, spec string, opts ...TriggerOption[Type, Status]) error

	// Await is a blocking call that returns the typed Record when the workflow of the specified run ID reaches the
	// specified status.
	Await(ctx context.Context, foreignID, runID string, status Status, opts ...AwaitOption) (*Record[Type, Status], error)

	// Callback can be used if Builder.AddCallback has been defined for the provided status. The data in the reader
	// will be passed to the CallbackFunc that you specify and so the serialisation and deserialisation is in the
	// hands of the user.
	Callback(ctx context.Context, foreignID string, status Status, payload io.Reader) error

	// Run must be called in order to start up all the background consumers / consumers required to run the workflow. Run
	// only needs to be called once. Any subsequent calls to run are safe and are noop.
	Run(ctx context.Context)

	// Stop tells the workflow to shut down gracefully.
	Stop()
}

type Workflow[Type any, Status ~string] struct {
	Name string

	ctx    context.Context
	cancel context.CancelFunc

	clock                   clock.Clock
	defaultPollingFrequency time.Duration
	defaultErrBackOff       time.Duration

	calledRun bool
	once      sync.Once

	eventStreamerFn EventStreamer
	recordStore     RecordStore
	timeoutStore    TimeoutStore
	scheduler       RoleScheduler

	consumers map[Status][]consumerConfig[Type, Status]
	callback  map[Status][]callback[Type, Status]
	timeouts  map[Status]timeouts[Type, Status]

	connectorConfigs []connectorConfig[Type, Status]

	internalStateMu sync.Mutex
	// internalState holds the State of all expected consumers and timeout  go routines using their role names
	// as the key.
	internalState map[string]State

	graph         map[Status][]Status
	endPoints     map[Status]bool
	validStatuses map[Status]bool

	debugMode bool
}

func (w *Workflow[Type, Status]) Run(ctx context.Context) {
	// Ensure that the background consumers are only initialized once
	w.once.Do(func() {
		ctx, cancel := context.WithCancel(ctx)
		w.ctx = ctx
		w.cancel = cancel
		w.calledRun = true

		for currentStatus, consumers := range w.consumers {
			for _, p := range consumers {
				if p.ParallelCount < 2 {
					// Launch all consumers in runners
					go consumer(w, currentStatus, p, 1, 1)
				} else {
					// Run as sharded parallel consumers
					for i := 1; i <= p.ParallelCount; i++ {
						go consumer(w, currentStatus, p, i, p.ParallelCount)
					}
				}
			}
		}

		for status, timeouts := range w.timeouts {
			go timeoutPoller(ctx, w, status, timeouts)
			go timeoutAutoInserterConsumer(ctx, w, status, timeouts)
		}

		for _, config := range w.connectorConfigs {
			if config.parallelCount < 2 {
				// Launch all consumers in runners
				go connectorConsumer(ctx, w, &config, 1, 1)
			} else {
				// Run as sharded parallel consumers
				for i := 1; i <= config.parallelCount; i++ {
					go connectorConsumer(ctx, w, &config, i, config.parallelCount)
				}
			}
		}
	})
}

// run is a standardise way of running blocking calls forever with retry such as consumers that need to adhere to role scheduling
func (w *Workflow[Type, Status]) run(role string, process func(ctx context.Context) error, errBackOff time.Duration) {
	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	for {
		ctx, cancel, err := w.scheduler.Await(w.ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "error awaiting role", j.MKV{
				"role": role,
			}))
			time.Sleep(errBackOff)
			continue
		}

		w.updateState(role, StateRunning)

		if ctx.Err() != nil {
			// Gracefully exit when context has been cancelled
			if w.debugMode {
				log.Error(ctx, errors.Wrap(err, "shutting down", j.MKV{
					"role": role,
				}))
			}
			return
		}

		err = process(ctx)
		if errors.IsAny(err, ErrWorkflowShutdown, context.Canceled) {
			// Exit the process if ErrWorkflowShutdown or context.Canceled is returned
			return
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "process error", j.MKV{
				"role": role,
			}))
		} else if err == nil {
			// If process finishes successfully then release the role by cancelling the context can continuing.
			cancel()
			continue
		}

		select {
		case <-ctx.Done():
			cancel()
			continue
		case <-time.After(errBackOff):
			cancel()
		}
	}
}

// Stop cancels the context provided to all the background processes that the workflow launched and waits for all of
// them to shut down gracefully.
func (w *Workflow[Type, Status]) Stop() {
	if w.cancel == nil {
		return
	}

	// Cancel the parent context of the workflow to gracefully shutdown.
	w.cancel()

	for {
		var runningProcesses int
		for _, state := range w.States() {
			switch state {
			case StateUnknown, StateShutdown:
				continue
			default:
				runningProcesses++
			}
		}

		// Once all processes have exited then return
		if runningProcesses == 0 {
			return
		}
	}
}

func update(ctx context.Context, streamer EventStreamer, store RecordStore, wr *WireRecord) error {
	body, err := wr.ProtoMarshal()
	if err != nil {
		return err
	}

	e := Event{
		ForeignID: wr.ForeignID,
		Body:      body,
		Headers:   make(map[string]string),
	}

	err = store.Store(ctx, wr)
	if err != nil {
		return err
	}

	topic := Topic(wr.WorkflowName, wr.Status)
	producer := streamer.NewProducer(topic)
	err = producer.Send(ctx, &e)
	if err != nil {
		return err
	}

	return producer.Close()
}
