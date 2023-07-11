package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
)

func TriggerCallbackOn[T any, Payload any](t *testing.T, w *Workflow[T], foreignID, runID string, waitFor string, p Payload) {
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

func AwaitTimeoutInsert[T any](t *testing.T, w *Workflow[T], status string) {
	if t == nil {
		panic("AwaitTimeout can only be used for testing")
	}

	ctx := context.TODO()
	r, err := w.store.LastRecordForWorkflow(ctx, w.Name)
	jtest.RequireNil(t, err)

	runID, err := w.store.LastRunID(ctx, w.Name, r.ForeignID)
	jtest.RequireNil(t, err)

	timeouts := w.timeouts[status]
	pf := timeouts.PollingFrequency
	if pf.Nanoseconds() == 0 {
		pf = w.defaultPollingFrequency
	}

	time.Sleep(pf * 2)

	_, err = w.Await(ctx, r.ForeignID, runID, status, WithPollingFrequency(pf*2))
	jtest.RequireNil(t, err)
}
