package gettingstarted

import (
	"context"

	"github.com/andrewwormald/workflow"
)

type GettingStarted struct {
	ReadTheDocs       string
	FollowAnExample   string
	CreateAFunExample string
}

type Deps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
}

func GettingStartedWorkflow(d Deps) *workflow.Workflow[GettingStarted, string] {
	b := workflow.NewBuilder[GettingStarted, string]("getting started")

	b.AddStep("Started", func(ctx context.Context, r *workflow.Record[GettingStarted, string]) (bool, error) {
		r.Object.ReadTheDocs = "✅"
		return true, nil
	}, "Read the docs")

	b.AddStep("Read the docs", func(ctx context.Context, r *workflow.Record[GettingStarted, string]) (bool, error) {
		r.Object.FollowAnExample = "✅"
		return true, nil
	}, "Followed the example")

	b.AddStep("Followed the example", func(ctx context.Context, r *workflow.Record[GettingStarted, string]) (bool, error) {
		r.Object.CreateAFunExample = "✅"
		return true, nil
	}, "Created a fun example")

	return b.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
	)
}
