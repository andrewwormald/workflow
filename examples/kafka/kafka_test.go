package kafka_test

import (
	"context"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/examples/gettingstarted"
	"github.com/andrewwormald/workflow/examples/kafka"
	"github.com/luno/jettison/jtest"
	"testing"
)

func TestKafkaExampleWorkflow(t *testing.T) {
	wf := kafka.KafkaExampleWorkflow()
	t.Cleanup(wf.Stop)

	ctx := context.Background()
	wf.Run(ctx)

	foreignID := "82347982374982374"
	runID, err := wf.Trigger(ctx, foreignID, gettingstarted.StatusStarted)
	jtest.RequireNil(t, err)

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusReadTheDocs, gettingstarted.GettingStarted{
		ReadTheDocs: "✅",
	})

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusFollowedTheExample, gettingstarted.GettingStarted{
		ReadTheDocs:     "✅",
		FollowAnExample: "✅",
	})

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusCreatedAFunExample, gettingstarted.GettingStarted{
		ReadTheDocs:       "✅",
		FollowAnExample:   "✅",
		CreateAFunExample: "✅",
	})
}
