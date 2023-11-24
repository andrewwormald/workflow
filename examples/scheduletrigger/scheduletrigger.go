package scheduletrigger

import (
	"context"

	"k8s.io/utils/clock"

	"github.com/andrewwormald/workflow"
)

type Example struct {
	EmailConfirmed bool
}

type Deps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
	Clock         clock.Clock
}

func ExampleWorkflow(d Deps) *workflow.Workflow[Example, string] {
	b := workflow.NewBuilder[Example, string]("schedule trigger example")

	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[Example, string]) (bool, error) {
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
