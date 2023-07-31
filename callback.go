package workflow

import (
	"bytes"
	"context"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"io"
)

func processCallback[T any](ctx context.Context, w *Workflow[T], currentStatus, destinationStatus string, fn CallbackFunc[T], foreignID string, payload io.Reader) error {
	runID, err := w.store.LastRunID(ctx, w.Name, foreignID)
	if err != nil {
		return errors.Wrap(err, "failed to lookup last run id for callback", j.MKV{
			"foreign_id": foreignID,
		})
	}

	key := MakeKey(w.Name, foreignID, runID)
	latest, err := w.store.LookupLatest(ctx, key)
	if err != nil {
		return err
	}

	if latest.Status != currentStatus {
		// Latest record shows that the current status is in a different state than expected so skip.
		return nil
	}

	var t T
	err = Unmarshal(latest.Object, &t)
	if err != nil {
		return err
	}

	if payload == nil {
		// Ensure that an empty value implementation of io.Reader is passed in instead of nil to avoid panic and
		// rather allow an unmarshalling error.
		payload = bytes.NewReader([]byte{})
	}

	ok, err := fn(ctx, key, &t, payload)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	object, err := Marshal(&t)
	if err != nil {
		return err
	}

	isEnd := w.endPoints[destinationStatus]
	// isStart is only true at time of trigger and thus default set to false
	return w.store.Store(ctx, key, destinationStatus, object, false, isEnd)
}
