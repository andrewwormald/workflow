package workflow

import (
	"context"
	"strconv"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

var ErrStreamingClosed = errors.New("streaming closed")

func stream[T any](ctx context.Context, c Cursor, s Store, workflowName string, status string, p Process[T]) error {
	val, err := c.Get(ctx, cursorName(workflowName, status))
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

		rs, err := s.Batch(ctx, workflowName, status, cursor, 1000)
		if err != nil {
			return err
		}

		for _, r := range rs {
			var t T
			err := Unmarshal(r.Object, &t)
			if err != nil {
				return err
			}

			ok, err := p.Consumer(ctx, &t)
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

				err = s.Store(ctx, workflowName, r.ForeignID, p.DestinationStatus.String(), b)
				if err != nil {
					return err
				}
			}

			cursor = r.ID
		}

		err = c.Set(ctx, cursorName(workflowName, status), strconv.FormatInt(cursor, 10))
		if err != nil {
			return err
		}

		err = wait(ctx, time.Second)
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

func awaitWorkflowStatusByForeignID[T any](ctx context.Context, s Store, workflowName string, foreignID string, status string) (*T, error) {
	for {
		if ctx.Err() != nil {
			return nil, errors.Wrap(ErrStreamingClosed, "")
		}

		r, err := s.Find(ctx, workflowName, foreignID, status)
		if errors.Is(err, ErrRecordNotFound) {
			err = wait(ctx, time.Second)
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
