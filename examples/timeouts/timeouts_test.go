package timeouts_test

import (
	"context"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memstreamer"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
	"github.com/andrewwormald/workflow/examples/timeouts"
)

func TestTimeoutWorkflow(t *testing.T) {
	now := time.Now().UTC()
	clock := clocktesting.NewFakeClock(now)
	wf := timeouts.TimeoutWorkflow(timeouts.Deps{
		EventStreamer: memstreamer.New(),
		RecordStore:   memrecordstore.New(),
		TimeoutStore:  memtimeoutstore.New(),
		RoleScheduler: memrolescheduler.New(),
		Clock:         clock,
	})

	ctx := context.Background()
	wf.Run(ctx)

	foreignID := "andrew"
	runID, err := wf.Trigger(ctx, foreignID, "Start")
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, "Start", foreignID, runID)

	clock.Step(time.Hour)

	workflow.Require(t, wf, "End", foreignID, runID, timeouts.TimeoutExample{
		Now: clock.Now(),
	})
}
