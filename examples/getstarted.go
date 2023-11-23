package examples

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
