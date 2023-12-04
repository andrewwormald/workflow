package workflow_test

import (
	"context"
	"testing"

	"github.com/luno/jettison/jtest"

	"github.com/andrewwormald/workflow"
)

func TestVisualiser(t *testing.T) {
	b := workflow.NewBuilder[string, string]("example")
	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[string, string]) (bool, error) {
		return true, nil
	}, "Middle")

	b.AddStep("Middle", func(ctx context.Context, r *workflow.Record[string, string]) (bool, error) {
		return true, nil
	}, "End", workflow.WithParallelCount(3))

	wf := b.Build(nil, nil, nil, nil)

	err := workflow.MermaidDiagram(wf, "./testfiles/testgraph.md", workflow.LeftToRightDirection)
	jtest.RequireNil(t, err)
}
