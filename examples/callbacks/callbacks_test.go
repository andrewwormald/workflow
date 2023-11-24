package callbacks_test

import (
	"context"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/jtest"
	"testing"

	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memstreamer"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
	"github.com/andrewwormald/workflow/examples/callbacks"
)

func TestCallbackWorkflow(t *testing.T) {
	wf := callbacks.CallbackWorkflow(callbacks.Deps{
		EventStreamer: memstreamer.New(),
		RecordStore:   memrecordstore.New(),
		TimeoutStore:  memtimeoutstore.New(),
		RoleScheduler: memrolescheduler.New(),
	})
	t.Cleanup(wf.Stop)

	ctx := context.Background()
	wf.Run(ctx)

	foreignID := "andrew"
	runID, err := wf.Trigger(ctx, foreignID, "Start")
	jtest.RequireNil(t, err)

	workflow.TriggerCallbackOn(t, wf, foreignID, runID, "Start", callbacks.EmailConfirmationResponse{
		Confirmed: true,
	})

	workflow.Require(t, wf, foreignID, runID, "End", callbacks.CallbackExample{
		EmailConfirmed: true,
	})
}
