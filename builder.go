package workflow

import (
	"context"
	"k8s.io/utils/clock"
	"path"
	"time"
)

func NewBuilder[Type any, Status ~string](name string) *Builder[Type, Status] {
	return &Builder[Type, Status]{
		workflow: &Workflow[Type, Status]{
			Name:                    name,
			clock:                   clock.RealClock{},
			defaultPollingFrequency: 500 * time.Millisecond,
			defaultErrBackOff:       500 * time.Millisecond,
			consumers:               make(map[Status][]consumerConfig[Type, Status]),
			callback:                make(map[Status][]callback[Type, Status]),
			timeouts:                make(map[Status]timeouts[Type, Status]),
			validStatuses:           make(map[Status]bool),
			internalState:           make(map[string]State),
		},
	}
}

type Builder[Type any, Status ~string] struct {
	workflow *Workflow[Type, Status]
}

func (b *Builder[Type, Status]) AddStep(from Status, c ConsumerFunc[Type, Status], to Status, opts ...StepOption) {
	p := consumerConfig[Type, Status]{
		DestinationStatus: to,
		Consumer:          c,
	}

	var so stepOptions
	for _, opt := range opts {
		opt(&so)
	}

	if so.parallelCount > 0 {
		p.ParallelCount = so.parallelCount
	}

	p.PollingFrequency = b.workflow.defaultPollingFrequency
	if so.pollingFrequency.Nanoseconds() != 0 {
		p.PollingFrequency = so.pollingFrequency
	}

	p.ErrBackOff = b.workflow.defaultErrBackOff
	if so.errBackOff.Nanoseconds() != 0 {
		p.ErrBackOff = so.errBackOff
	}

	b.workflow.validStatuses[from] = true
	b.workflow.validStatuses[to] = true
	b.workflow.consumers[from] = append(b.workflow.consumers[from], p)
}

type stepOptions struct {
	parallelCount    int
	pollingFrequency time.Duration
	errBackOff       time.Duration
}

type StepOption func(so *stepOptions)

func WithParallelCount(instances int) StepOption {
	return func(so *stepOptions) {
		so.parallelCount = instances
	}
}

func WithStepPollingFrequency(d time.Duration) StepOption {
	return func(so *stepOptions) {
		so.pollingFrequency = d
	}
}

func WithStepErrBackOff(d time.Duration) StepOption {
	return func(so *stepOptions) {
		so.errBackOff = d
	}
}

func (b *Builder[Type, Status]) AddCallback(from Status, fn CallbackFunc[Type, Status], to Status) {
	c := callback[Type, Status]{
		DestinationStatus: to,
		CallbackFunc:      fn,
	}

	b.workflow.validStatuses[from] = true
	b.workflow.validStatuses[to] = true
	b.workflow.callback[from] = append(b.workflow.callback[from], c)
}

type timeoutOptions struct {
	pollingFrequency time.Duration
	errBackOff       time.Duration
}

type TimeoutOption func(so *timeoutOptions)

func WithTimeoutPollingFrequency(d time.Duration) TimeoutOption {
	return func(to *timeoutOptions) {
		to.pollingFrequency = d
	}
}

func WithTimeoutErrBackOff(d time.Duration) TimeoutOption {
	return func(to *timeoutOptions) {
		to.errBackOff = d
	}
}

func (b *Builder[Type, Status]) AddTimeout(from Status, timer TimerFunc[Type, Status], tf TimeoutFunc[Type, Status], to Status, opts ...TimeoutOption) {
	timeouts := b.workflow.timeouts[from]

	t := timeout[Type, Status]{
		DestinationStatus: to,
		TimerFunc:         timer,
		TimeoutFunc:       tf,
	}

	var topt timeoutOptions
	for _, opt := range opts {
		opt(&topt)
	}

	timeouts.PollingFrequency = b.workflow.defaultPollingFrequency
	if topt.pollingFrequency.Nanoseconds() != 0 {
		timeouts.PollingFrequency = topt.pollingFrequency
	}

	timeouts.ErrBackOff = b.workflow.defaultErrBackOff
	if topt.errBackOff.Nanoseconds() != 0 {
		timeouts.ErrBackOff = topt.errBackOff
	}

	timeouts.Transitions = append(timeouts.Transitions, t)

	b.workflow.validStatuses[from] = true
	b.workflow.validStatuses[to] = true
	b.workflow.timeouts[from] = timeouts
}

func (b *Builder[Type, Status]) ConnectWorkflow(workflowName string, status string, stream EventStreamer, filter ConnectorFilter, consumer ConnectorConsumerFunc[Type, Status], to Status, opts ...StepOption) {
	var stepOptions stepOptions
	for _, opt := range opts {
		opt(&stepOptions)
	}
	b.workflow.validStatuses[to] = true
	b.workflow.connectorConfigs = append(b.workflow.connectorConfigs, connectorConfig[Type, Status]{
		workflowName:     workflowName,
		status:           status,
		stream:           stream,
		filter:           filter,
		consumer:         consumer,
		to:               string(to),
		pollingFrequency: stepOptions.pollingFrequency,
		errBackOff:       stepOptions.errBackOff,
		parallelCount:    stepOptions.parallelCount,
	})
}

func (b *Builder[Type, Status]) Build(eventStreamer EventStreamer, recordStore RecordStore, timeoutStore TimeoutStore, roleScheduler RoleScheduler, opts ...BuildOption) *Workflow[Type, Status] {
	b.workflow.eventStreamerFn = eventStreamer
	b.workflow.recordStore = recordStore
	b.workflow.timeoutStore = timeoutStore
	b.workflow.scheduler = roleScheduler

	var bo buildOptions
	for _, opt := range opts {
		opt(&bo)
	}

	if bo.clock != nil {
		b.workflow.clock = bo.clock
	}

	if b.workflow.defaultPollingFrequency.Milliseconds() == 0 {
		b.workflow.defaultPollingFrequency = time.Second
	}

	b.workflow.graph = b.buildGraph()
	b.workflow.endPoints = b.determineEndPoints(b.workflow.graph)
	b.workflow.debugMode = bo.debugMode

	return b.workflow
}

type buildOptions struct {
	clock     clock.Clock
	debugMode bool
}

type BuildOption func(w *buildOptions)

func WithClock(c clock.Clock) BuildOption {
	return func(bo *buildOptions) {
		bo.clock = c
	}
}

func WithDebugMode() BuildOption {
	return func(bo *buildOptions) {
		bo.debugMode = true
	}
}

func (b *Builder[Type, Status]) buildGraph() map[Status][]Status {
	graph := make(map[Status][]Status)
	dedupe := make(map[string]bool)
	for s, i := range b.workflow.consumers {
		for _, p := range i {
			key := path.Join(string(s), string(p.DestinationStatus))
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], p.DestinationStatus)
			dedupe[key] = true
		}
	}

	for s, i := range b.workflow.callback {
		for _, c := range i {
			key := path.Join(string(s), string(c.DestinationStatus))
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], c.DestinationStatus)
			dedupe[key] = true
		}
	}

	for s, t := range b.workflow.timeouts {
		for _, t := range t.Transitions {
			key := path.Join(string(s), string(t.DestinationStatus))
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], t.DestinationStatus)
			dedupe[key] = true
		}
	}

	return graph
}

func (b *Builder[Type, Status]) determineEndPoints(graph map[Status][]Status) map[Status]bool {
	endpoints := make(map[Status]bool)
	for _, destinations := range graph {
		for _, destination := range destinations {
			_, ok := graph[destination]
			if !ok {
				// end points are nodes that do not have any of their own transitions to transition to.
				endpoints[destination] = true
			}
		}
	}

	return endpoints
}

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
	return func(ctx context.Context, r *Record[Type, Status], now time.Time) (time.Time, error) {
		return now.Add(duration), nil
	}
}

func TimeTimerFunc[Type any, Status ~string](t time.Time) TimerFunc[Type, Status] {
	return func(ctx context.Context, r *Record[Type, Status], now time.Time) (time.Time, error) {
		return t, nil
	}
}
