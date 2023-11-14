package workflow

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

type EventStreamerConstructor interface {
	NewProducer(topic string) Producer
	NewConsumer(topic string, name string) Consumer
}

// TODO: Add send meta data like stack traces
type Producer interface {
	Send(ctx context.Context, r *WireRecord) error
	Close() error
}

type Consumer interface {
	Recv(ctx context.Context) (*WireRecord, Ack, error)
	Close() error
}

// Ack is used for the event streamer to update it's cursor of what messages have
// been consumed. If Ack is not called then the event streamer, depending on implementation,
// will likely not keep track of which records / events have been consumed.
type Ack func() error

func update(ctx context.Context, streamFn EventStreamerConstructor, store RecordStore, wr *WireRecord) error {
	err := store.Store(ctx, wr)
	if err != nil {
		return err
	}

	topic := Topic(wr.WorkflowName, wr.Status)
	producer := streamFn.NewProducer(topic)
	err = producer.Send(ctx, wr)
	if err != nil {
		return err
	}

	return producer.Close()
}

func runStepConsumerForever[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], p process[Type, Status], status Status, role string) error {
	topic := Topic(w.Name, string(status))
	stream := w.eventStreamerFn.NewConsumer(topic, role)
	defer stream.Close()

	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrStreamingClosed, "")
		}

		r, ack, err := stream.Recv(ctx)
		if err != nil {
			return err
		}

		latest, err := w.recordStore.Latest(ctx, r.WorkflowName, r.ForeignID)
		if err != nil {
			return err
		}

		if latest.Status != r.Status {
			err = ack()
			if err != nil {
				return err
			}
			continue
		}

		if latest.RunID != r.RunID {
			err = ack()
			if err != nil {
				return err
			}
			continue
		}

		var t Type
		err = Unmarshal(r.Object, &t)
		if err != nil {
			return err
		}

		record := Record[Type, Status]{
			WireRecord: *r,
			Status:     Status(r.Status),
			Object:     &t,
		}

		ok, err := p.Consumer(ctx, &record)
		if err != nil {
			return errors.Wrap(err, "failed to process", j.MKV{
				"workflow_name":      r.WorkflowName,
				"foreign_id":         r.ForeignID,
				"current_status":     r.Status,
				"destination_status": p.DestinationStatus,
			})
		}

		if ok {
			b, err := Marshal(&record.Object)
			if err != nil {
				return err
			}

			isEnd := w.endPoints[p.DestinationStatus]
			wr := &WireRecord{
				RunID:        record.RunID,
				WorkflowName: record.WorkflowName,
				ForeignID:    record.ForeignID,
				Status:       string(p.DestinationStatus),
				IsStart:      false,
				IsEnd:        isEnd,
				Object:       b,
				CreatedAt:    record.CreatedAt,
			}

			err = update(ctx, w.eventStreamerFn, w.recordStore, wr)
			if err != nil {
				return err
			}
		}

		err = ack()
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

func awaitWorkflowStatusByForeignID[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, foreignID, runID string, role string, pollFrequency time.Duration) (*Record[Type, Status], error) {
	topic := Topic(w.Name, string(status))
	stream := w.eventStreamerFn.NewConsumer(topic, role)
	defer stream.Close()

	for {
		if ctx.Err() != nil {
			return nil, errors.Wrap(ErrStreamingClosed, "")
		}

		r, ack, err := stream.Recv(ctx)
		if err != nil {
			return nil, err
		}

		switch true {
		// If the record doesn't match the status, foreignID, and runID then sleep and try again
		case r.Status != string(status), r.ForeignID != foreignID, r.RunID != runID:
			// Increment the offset / cursor to process new events
			err = ack()
			if err != nil {
				return nil, err
			}

			continue
		}

		var t Type
		err = Unmarshal(r.Object, &t)
		if err != nil {
			return nil, err
		}

		return &Record[Type, Status]{
			WireRecord: *r,
			Status:     Status(r.Status),
			Object:     &t,
		}, ack()
	}
}

// pollTimeouts attempts to find the very next
func pollTimeouts[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, timeouts timeouts[Type, Status]) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrStreamingClosed, "")
		}

		expiredTimeouts, err := w.timeoutStore.ListValid(ctx, w.Name, string(status), w.clock.Now())
		if err != nil {
			return err
		}

		for _, expiredTimeout := range expiredTimeouts {
			r, err := w.recordStore.Latest(ctx, expiredTimeout.WorkflowName, expiredTimeout.ForeignID)
			if err != nil {
				return err
			}

			if r.Status != string(status) {
				// Object has been updated already. Mark timeout as cancelled as it is no longer valid.
				err = w.timeoutStore.Cancel(ctx, expiredTimeout.WorkflowName, expiredTimeout.ForeignID, expiredTimeout.RunID, expiredTimeout.Status)
				if err != nil {
					return err
				}
			}

			var t Type
			err = Unmarshal(r.Object, &t)
			if err != nil {
				return err
			}

			record := Record[Type, Status]{
				WireRecord: *r,
				Status:     Status(r.Status),
				Object:     &t,
			}

			for _, config := range timeouts.Transitions {
				ok, err := config.TimeoutFunc(ctx, &record, w.clock.Now())
				if err != nil {
					return err
				}

				if ok {
					object, err := Marshal(&t)
					if err != nil {
						return err
					}

					wr := &WireRecord{
						WorkflowName: record.WorkflowName,
						ForeignID:    record.ForeignID,
						RunID:        record.RunID,
						Status:       string(config.DestinationStatus),
						IsStart:      false,
						IsEnd:        w.endPoints[config.DestinationStatus],
						Object:       object,
						CreatedAt:    record.CreatedAt,
					}

					err = update(ctx, w.eventStreamerFn, w.recordStore, wr)
					if err != nil {
						return err
					}

					// Mark timeout as having been executed (aka completed) only in the case that true is returned.
					err = w.timeoutStore.Complete(ctx, record.WorkflowName, record.ForeignID, record.RunID, string(record.Status))
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
