package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TriggerCallbackOn[Type any, Status ~string, Payload any](t *testing.T, w *Workflow[Type, Status], foreignID, runID string, waitFor Status, p Payload) {
	if t == nil {
		panic("TriggerCallbackOn can only be used for testing")
	}

	ctx := context.TODO()

	_, err := w.Await(ctx, foreignID, runID, waitFor)
	jtest.RequireNil(t, err)

	b, err := json.Marshal(p)
	jtest.RequireNil(t, err)

	err = w.Callback(ctx, foreignID, waitFor, bytes.NewReader(b))
	jtest.RequireNil(t, err)
}

func AwaitTimeoutInsert[Type any, Status ~string](t *testing.T, w *Workflow[Type, Status], status Status, foreignID, runID string) {
	if t == nil {
		panic("AwaitTimeout can only be used for testing")
	}

	var found bool
	for !found {
		if w.ctx.Err() != nil {
			return
		}

		ls, err := w.timeoutStore.List(w.ctx, w.Name)
		jtest.RequireNil(t, err)

		for _, l := range ls {
			if l.Status != string(status) {
				continue
			}

			if l.ForeignID != foreignID {
				continue
			}

			if l.RunID != runID {
				continue
			}

			found = true
			break
		}
	}
}

func Require[Type any, Status ~string](t *testing.T, w *Workflow[Type, Status], status Status, foreignID, runID string, expected Type) {
	if t == nil {
		panic("Require can only be used for testing")
	}

	_, ok := w.validStatuses[status]
	if !ok {
		t.Error(fmt.Sprintf(`Status provided is not configured for workflow: "%v" (Workflow: %v)`, status, w.Name))
		return
	}

	ctx := context.TODO()
	actual, err := w.Await(ctx, foreignID, runID, status)
	jtest.RequireNil(t, err)

	require.Equal(t, expected, *actual.Object)
}
