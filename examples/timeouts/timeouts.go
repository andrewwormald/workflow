package timeouts

import (
	"context"
	"time"

	"k8s.io/utils/clock"

	"github.com/andrewwormald/workflow"
)

type TimeoutExample struct {
	Now time.Time
}

type Deps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
	Clock         clock.Clock
}

func TimeoutWorkflow(d Deps) *workflow.Workflow[TimeoutExample, string] {
	b := workflow.NewBuilder[TimeoutExample, string]("timeout example")

	b.AddTimeout("Start", func(ctx context.Context, r *workflow.Record[TimeoutExample, string], now time.Time) (time.Time, error) {
		// Using "now" over time.Now() allows for you to specify a clock for testing.
		return now.Add(time.Hour), nil
	}, func(ctx context.Context, r *workflow.Record[TimeoutExample, string], now time.Time) (bool, error) {
		r.Object.Now = now
		return true, nil
	}, "End")

	return b.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
		workflow.WithClock(d.Clock),
	)
}
