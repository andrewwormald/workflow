package callbacks

import (
	"context"
	"encoding/json"
	"io"

	"github.com/andrewwormald/workflow"
)

type Example struct {
	EmailConfirmed bool
}

type EmailConfirmationResponse struct {
	Confirmed bool
}

type Deps struct {
	EventStreamer workflow.EventStreamer
	RecordStore   workflow.RecordStore
	TimeoutStore  workflow.TimeoutStore
	RoleScheduler workflow.RoleScheduler
}

func ExampleWorkflow(d Deps) *workflow.Workflow[Example, string] {
	b := workflow.NewBuilder[Example, string]("callback example")

	b.AddCallback("Start", func(ctx context.Context, r *workflow.Record[Example, string], reader io.Reader) (bool, error) {
		b, err := io.ReadAll(reader)
		if err != nil {
			return false, err
		}

		var e EmailConfirmationResponse
		err = json.Unmarshal(b, &e)
		if err != nil {
			return false, err
		}

		r.Object.EmailConfirmed = e.Confirmed

		return true, nil
	}, "End")

	return b.Build(
		d.EventStreamer,
		d.RecordStore,
		d.TimeoutStore,
		d.RoleScheduler,
	)
}
