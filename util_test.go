package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	clock_testing "k8s.io/utils/clock/testing"
)

func TriggerCallbackOn[T any, Payload any](t *testing.T, w *Workflow[T], foreignID, runID string, waitFor Status, p Payload) {
	ctx := context.TODO()

	_, err := w.Await(ctx, foreignID, runID, waitFor)
	jtest.RequireNil(t, err)

	b, err := json.Marshal(p)
	jtest.RequireNil(t, err)

	err = w.Callback(ctx, foreignID, waitFor, bytes.NewReader(b))
	jtest.RequireNil(t, err)
}

func ChangeTimeOn[T any](t *testing.T, w *Workflow[T], foreignID, runID string, waitFor Status, newTime time.Time) {
	ctx := context.TODO()

	_, err := w.Await(ctx, foreignID, runID, waitFor)
	jtest.RequireNil(t, err)

	// Provide enough time for the timeout to be created
	time.Sleep(w.pollingFrequency * 2)

	// Ensure to update the provided time to account for the sleeping to ensure the timeout is inserted before the
	// clock is changed.
	newTime = newTime.Add(w.pollingFrequency * 2)

	w.clock = clock_testing.NewFakeClock(newTime)
}
