package workflow

import (
	"context"
	"strconv"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

func streamAndConsume[T any](ctx context.Context, w *Workflow[T], status string, p process[T], shard, totalShards int64) error {
	cName := cursorName(w.Name, status, shard, totalShards)

	val, err := w.cursor.Get(ctx, cName)
	if errors.Is(err, ErrCursorNotFound) {
		// Set to default value of a string 0
		val = "0"
	} else if err != nil {
		return err
	}

	cursor, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrStreamingClosed, "")
		}

		rs, err := w.store.Batch(ctx, w.Name, status, cursor, 1000)
		if err != nil {
			return err
		}

		for _, r := range rs {
			if totalShards > 1 {
				if r.ID%totalShards != shard {
					// Ensure this consumer is intended to process this event
					continue
				}
			}

			key := MakeKey(r.WorkflowName, r.ForeignID, r.RunID)
			latest, err := w.store.LookupLatest(ctx, key)
			if err != nil {
				return err
			}

			if latest.Status != status {
				// Event is out of date, for idempotence skip the event record and only process the event record
				// that matches the latest record inserted
				continue
			}

			var t T
			err = Unmarshal(r.Object, &t)
			if err != nil {
				return err
			}

			ok, err := p.Consumer(ctx, key, &t)
			if err != nil {
				return errors.Wrap(err, "failed to process", j.MKV{
					"workflow_name":      r.WorkflowName,
					"foreign_id":         r.ForeignID,
					"current_status":     r.Status,
					"destination_status": p.DestinationStatus,
				})
			}

			if ok {
				b, err := Marshal(&t)
				if err != nil {
					return err
				}

				isEnd := w.endPoints[p.DestinationStatus.String()]

				// isStart is only true at time of trigger and thus default set to false
				err = w.store.Store(ctx, key, p.DestinationStatus.String(), b, false, isEnd)
				if err != nil {
					return err
				}
			}

			cursor = r.ID
		}

		err = w.cursor.Set(ctx, cName, strconv.FormatInt(cursor, 10))
		if err != nil {
			return err
		}

		err = wait(ctx, p.PollingFrequency)
		if err != nil {
			return err
		}
	}
}

func wait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return nil
	}

	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return errors.Wrap(ErrStreamingClosed, ctx.Err().Error())
	case <-t.C:
		return nil
	}
}

func awaitWorkflowStatusByForeignID[T any](ctx context.Context, w *Workflow[T], key Key, status string, pollFrequency time.Duration) (*T, error) {
	for {
		if ctx.Err() != nil {
			return nil, errors.Wrap(ErrStreamingClosed, "")
		}

		r, err := w.store.Find(ctx, key, status)
		if errors.Is(err, ErrRecordNotFound) {
			err = wait(ctx, pollFrequency)
			if err != nil {
				return nil, err
			}

			// Try again
			continue
		} else if err != nil {
			return nil, err
		}

		var t T
		err = Unmarshal(r.Object, &t)
		if err != nil {
			return nil, err
		}

		return &t, nil
	}
}

// pollTimeouts attempts to find the very next
func pollTimeouts[T any](ctx context.Context, w *Workflow[T], status string, timeouts timeouts[T]) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrStreamingClosed, "")
		}

		expiredTimeouts, err := w.store.ListValidTimeouts(ctx, w.Name, status, w.clock.Now())
		if err != nil {
			return err
		}

		for _, expiredTimeout := range expiredTimeouts {
			key := MakeKey(expiredTimeout.WorkflowName, expiredTimeout.ForeignID, expiredTimeout.RunID)
			r, err := w.store.LookupLatest(ctx, key)
			if err != nil {
				return err
			}

			if r.Status != status {
				// Object has been updated already. Mark timeout as cancelled as it is no longer valid.
				err = w.store.CancelTimeout(ctx, expiredTimeout.ID)
				if err != nil {
					return err
				}
			}

			var t T
			err = Unmarshal(r.Object, &t)
			if err != nil {
				return err
			}

			for _, config := range timeouts.Transitions {
				ok, err := config.TimeoutFunc(ctx, key, &t, time.Now())
				if err != nil {
					return err
				}

				if ok {
					b, err := Marshal(&t)
					if err != nil {
						return err
					}

					isEnd := w.endPoints[config.DestinationStatus.String()]

					// isStart is only true at time of trigger and thus default set to false
					err = w.store.Store(ctx, key, config.DestinationStatus.String(), b, false, isEnd)
					if err != nil {
						return err
					}

					// Mark timeout as having been executed (aka completed) only in the case that true is returned.
					err = w.store.CompleteTimeout(ctx, expiredTimeout.ID)
					if err != nil {
						return err
					}
				}
			}
		}

		// Use the fastest polling frequency to sleep and try again if there are no timeouts available
		err = wait(ctx, timeouts.PollingFrequency)
		if err != nil {
			return err
		}
	}
}
