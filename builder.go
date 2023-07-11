package workflow

import (
	"k8s.io/utils/clock"
	"path"
	"time"
)

func NewBuilder[T any](name string, store Store, cursor Cursor) *Builder[T] {
	return &Builder[T]{
		workflow: &Workflow[T]{
			Name:                    name,
			clock:                   clock.RealClock{},
			store:                   store,
			cursor:                  cursor,
			defaultPollingFrequency: 500 * time.Millisecond,
			defaultErrBackOff:       500 * time.Millisecond,
			processes:               make(map[string][]process[T]),
			callback:                make(map[string][]callback[T]),
			timeouts:                make(map[string]timeouts[T]),
		},
	}
}

type Builder[T any] struct {
	workflow *Workflow[T]
}

func (b *Builder[T]) AddStep(from string, c ConsumerFunc[T], to string, opts ...StepOption) {
	p := process[T]{
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

	b.workflow.processes[from] = append(b.workflow.processes[from], p)
}

type stepOptions struct {
	parallelCount    int64
	pollingFrequency time.Duration
	errBackOff       time.Duration
}

type StepOption func(so *stepOptions)

func WithParallelCount(instances int64) StepOption {
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

func (b *Builder[T]) AddCallback(from string, fn CallbackFunc[T], to string) {
	c := callback[T]{
		DestinationStatus: to,
		CallbackFunc:      fn,
	}

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

func (b *Builder[T]) AddTimeout(from string, tf TimeoutFunc[T], time time.Time, to string, opts ...TimeoutOption) {
	timeouts := b.workflow.timeouts[from]

	duration := time.Sub(b.workflow.clock.Now())

	t := timeout[T]{
		DestinationStatus: to,
		Duration:          duration,
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

	b.workflow.timeouts[from] = timeouts
}

func (b *Builder[T]) AddTimeoutWithDuration(from string, tf TimeoutFunc[T], duration time.Duration, to string, opts ...TimeoutOption) {
	timeouts := b.workflow.timeouts[from]

	t := timeout[T]{
		DestinationStatus: to,
		Duration:          duration,
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

	b.workflow.timeouts[from] = timeouts
}

func (b *Builder[T]) Build(opts ...BuildOption) *Workflow[T] {
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

	return b.workflow
}

type buildOptions struct {
	clock clock.Clock
}

type BuildOption func(w *buildOptions)

func WithClock(c clock.Clock) BuildOption {
	return func(bo *buildOptions) {
		bo.clock = c
	}
}

func (b *Builder[T]) buildGraph() map[string][]string {
	graph := make(map[string][]string)
	dedupe := make(map[string]bool)
	for s, i := range b.workflow.processes {
		for _, p := range i {
			key := path.Join(s, p.DestinationStatus)
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], p.DestinationStatus)
			dedupe[key] = true
		}
	}

	for s, i := range b.workflow.callback {
		for _, c := range i {
			key := path.Join(s, c.DestinationStatus)
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], c.DestinationStatus)
			dedupe[key] = true
		}
	}

	for s, t := range b.workflow.timeouts {
		for _, t := range t.Transitions {
			key := path.Join(s, t.DestinationStatus)
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], t.DestinationStatus)
			dedupe[key] = true
		}
	}

	return graph
}

func (b *Builder[T]) determineEndPoints(graph map[string][]string) map[string]bool {
	endpoints := make(map[string]bool)
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
