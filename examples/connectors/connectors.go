package connectors

import (
	"context"

	"github.com/andrewwormald/workflow"
)

type WorkflowADeps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
}

type TypeA struct {
	Value string
}

func WorkflowA(d WorkflowADeps) *workflow.Workflow[TypeA, string] {
	builder := workflow.NewBuilder[TypeA, string]("workflow A")

	builder.AddStep("Start", func(ctx context.Context, r *workflow.Record[TypeA, string]) (bool, error) {
		r.Object.Value = "Hello"
		return true, nil
	}, "Middle")

	builder.AddStep("Middle", func(ctx context.Context, r *workflow.Record[TypeA, string]) (bool, error) {
		r.Object.Value += " World"
		return true, nil
	}, "End")

	return builder.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
	)
}

type WorkflowBDeps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler

	WorkflowAStreamer workflow.EventStreamer
}

type TypeB struct {
	Value string
}

func WorkflowB(d WorkflowBDeps) *workflow.Workflow[TypeB, string] {
	builder := workflow.NewBuilder[TypeB, string]("workflow B")

	builder.AddStep("Start", func(ctx context.Context, r *workflow.Record[TypeB, string]) (bool, error) {
		return true, nil
	}, "Waiting for workflow A")

	builder.ConnectWorkflow(
		"workflow A",
		"End",
		d.WorkflowAStreamer,
		func(ctx context.Context, e *workflow.Event) (foreignID string, err error) {
			return e.ForeignID, nil
		},
		func(ctx context.Context, r *workflow.Record[TypeB, string], e *workflow.Event) (bool, error) {
			wr, err := workflow.UnmarshalRecord(e.Body)
			if err != nil {
				return false, err
			}

			var objectA TypeB
			err = workflow.Unmarshal(wr.Object, &objectA)
			if err != nil {
				return false, err
			}

			r.Object.Value = objectA.Value

			return true, nil
		},
		"Finished waiting",
	)

	return builder.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
	)
}
