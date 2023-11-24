package scheduletrigger

import (
	"context"
	"k8s.io/utils/clock"

	"github.com/andrewwormald/workflow"
)

type ScheduleExample struct {
	EmailConfirmed bool
}

type Deps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
	Clock         clock.Clock
}

func ScheduleTriggerExampleWorkflow(d Deps) *workflow.Workflow[ScheduleExample, string] {
	b := workflow.NewBuilder[ScheduleExample, string]("schedule trigger example")

	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[ScheduleExample, string]) (bool, error) {
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
