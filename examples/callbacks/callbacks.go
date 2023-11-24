package callbacks

import (
	"context"
	"encoding/json"
	"io"

	"github.com/andrewwormald/workflow"
)

type CallbackExample struct {
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

func CallbackExampleWorkflow(d Deps) *workflow.Workflow[CallbackExample, string] {
	b := workflow.NewBuilder[CallbackExample, string]("callback example")

	b.AddCallback("Start", func(ctx context.Context, r *workflow.Record[CallbackExample, string], reader io.Reader) (bool, error) {
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
