package gettingstarted

import (
	"context"

	"github.com/andrewwormald/workflow"
)

type Status string

var (
	StatusUnknown            Status = ""
	StatusStarted            Status = "Started"
	StatusReadTheDocs        Status = "Read the docs"
	StatusFollowedTheExample Status = "Followed the example"
	StatusCreatedAFunExample Status = "Created a fun example"
)

func GettingStartedWithEnumWorkflow(d Deps) *workflow.Workflow[GettingStarted, Status] {
	b := workflow.NewBuilder[GettingStarted, Status]("getting started")

	b.AddStep(StatusStarted, func(ctx context.Context, r *workflow.Record[GettingStarted, Status]) (bool, error) {
		r.Object.ReadTheDocs = "✅"
		return true, nil
	}, StatusReadTheDocs)

	b.AddStep(StatusReadTheDocs, func(ctx context.Context, r *workflow.Record[GettingStarted, Status]) (bool, error) {
		r.Object.FollowAnExample = "✅"
		return true, nil
	}, StatusFollowedTheExample)

	b.AddStep(StatusFollowedTheExample, func(ctx context.Context, r *workflow.Record[GettingStarted, Status]) (bool, error) {
		r.Object.CreateAFunExample = "✅"
		return true, nil
	}, StatusCreatedAFunExample)

	return b.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
	)
}
