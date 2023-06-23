package workflow

import (
	"context"
	"io"
)

func processCallback[T any](ctx context.Context, w *Workflow[T], destinationStatus string, fn CallbackFunc[T], foreignID string, payload io.Reader) error {
	runID, err := w.store.LastRunID(ctx, w.Name, foreignID)
	if err != nil {
		return err
	}

	key := MakeKey(w.Name, foreignID, runID)
	latest, err := w.store.LookupLatest(ctx, key)
	if err != nil {
		return err
	}

	var t T
	err = Unmarshal(latest.Object, &t)
	if err != nil {
		return err
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

	isStart := w.startingPoints[destinationStatus]
	isEnd := w.endPoints[destinationStatus]
	return w.store.Store(ctx, key, destinationStatus, object, isStart, isEnd)
}
