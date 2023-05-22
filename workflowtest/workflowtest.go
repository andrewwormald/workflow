package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/jtest"
	"testing"
)

func TriggerCallbackOn[T any, Payload any](t *testing.T, w *workflow.Workflow[T], foreignID string, waitFor workflow.Status, p Payload) {
	ctx := context.TODO()

	_, err := w.Await(ctx, foreignID, waitFor)
	jtest.RequireNil(t, err)

	b, err := json.Marshal(p)
	jtest.RequireNil(t, err)

	err = w.Callback(ctx, foreignID, waitFor, bytes.NewReader(b))
	jtest.RequireNil(t, err)
}
