package workflow

import (
	"context"
	"io"
)

func processCallback[T any](ctx context.Context, w *Workflow[T], destinationStatus string, fn CallbackFunc[T], foreignID string, payload io.Reader) error {
	latest, err := w.store.LookupLatest(ctx, w.Name, foreignID)
	if err != nil {
		return err
	}

	var t T
	err = Unmarshal(latest.Object, &t)
	if err != nil {
		return err
	}

	ok, err := fn(ctx, &t, payload)
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

	return w.store.Store(ctx, w.Name, foreignID, destinationStatus, object)
}
