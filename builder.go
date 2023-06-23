package workflow

import (
	"path"
	"time"
)

type Builder[T any] struct {
	workflow *Workflow[T]
}

func (b *Builder[T]) AddStep(from Status, c ConsumerFunc[T], to Status, opts ...StepOption) {
	p := process[T]{
		Consumer:          c,
		DestinationStatus: to,
	}

	var so stepOptions
	for _, opt := range opts {
		opt(&so)
	}

	if so.parallelCount > 0 {
		p.ParallelCount = so.parallelCount
	}

	b.workflow.processes[from.String()] = append(b.workflow.processes[from.String()], p)
}

type stepOptions struct {
	parallelCount int64
}

type StepOption func(so *stepOptions)

func WithParallelCount(instances int64) StepOption {
	return func(so *stepOptions) {
		so.parallelCount = instances
	}
}

func (b *Builder[T]) AddCallback(from Status, fn CallbackFunc[T], to Status) {
	b.workflow.callback[from.String()] = append(b.workflow.callback[from.String()], callback[T]{
		DestinationStatus: to,
		CallbackFunc:      fn,
	})
}

func (b *Builder[T]) AddTimeout(from Status, tf TimeoutFunc[T], duration time.Duration, to Status) {
	b.workflow.timeouts[from.String()] = append(b.workflow.timeouts[from.String()], timeout[T]{
		DestinationStatus: to,
		Duration:          duration,
		TimeoutFunc:       tf,
	})
}

func (b *Builder[T]) Complete() *Workflow[T] {
	if b.workflow.pollingFrequency.Milliseconds() == 0 {
		b.workflow.pollingFrequency = time.Millisecond * 500
	}

	b.workflow.graph = b.buildGraph()
	b.workflow.startingPoints = b.determineStartingPoints(b.workflow.graph)
	b.workflow.endPoints = b.determineEndPoints(b.workflow.graph)

	return b.workflow
}

func (b *Builder[T]) buildGraph() map[string][]string {
	graph := make(map[string][]string)
	dedupe := make(map[string]bool)
	for s, i := range b.workflow.processes {
		for _, p := range i {
			key := path.Join(s, p.DestinationStatus.String())
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], p.DestinationStatus.String())
			dedupe[key] = true
		}
	}

	for s, i := range b.workflow.callback {
		for _, c := range i {
			key := path.Join(s, c.DestinationStatus.String())
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], c.DestinationStatus.String())
			dedupe[key] = true
		}
	}

	for s, i := range b.workflow.timeouts {
		for _, t := range i {
			key := path.Join(s, t.DestinationStatus.String())
			if dedupe[key] {
				continue
			}

			graph[s] = append(graph[s], t.DestinationStatus.String())
			dedupe[key] = true
		}
	}

	return graph
}

func (b *Builder[T]) determineStartingPoints(graph map[string][]string) map[string]bool {
	startingPoints := make(map[string]bool)
	for from, destinations := range graph {
		_, ok := startingPoints[from]
		if !ok {
			startingPoints[from] = true
		}

		for _, destination := range destinations {
			_, ok := startingPoints[destination]

			// If the starting point is a destination for a node on the graph then remove it from a starting point
			startingPoints[destination] = ok
		}
	}

	// Remove all false entries after full evaluation
	for s, val := range startingPoints {
		if !val {
			delete(startingPoints, s)
		}
	}

	return startingPoints
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
