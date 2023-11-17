package workflow

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

type EventStreamerConstructor interface {
	NewProducer(topic string) Producer
	NewConsumer(topic string, name string, opts ...ConsumerOption) Consumer
}

func update(ctx context.Context, streamFn EventStreamerConstructor, store RecordStore, wr *WireRecord) error {
	body, err := wr.ProtoMarshal()
	if err != nil {
		return err
	}

	e := Event{
		ForeignID: wr.ForeignID,
		Body:      body,
		Headers:   make(map[string]string),
	}

	err = store.Store(ctx, wr)
	if err != nil {
		return err
	}

	topic := Topic(wr.WorkflowName, wr.Status)
	producer := streamFn.NewProducer(topic)
	err = producer.Send(ctx, &e)
	if err != nil {
		return err
	}

	return producer.Close()
}

func runStepConsumerForever[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], p process[Type, Status], status Status, role string, shard, totalShards int) error {
	pollFrequency := w.defaultPollingFrequency
	if p.PollingFrequency.Nanoseconds() != 0 {
		pollFrequency = p.PollingFrequency
	}

	topic := Topic(w.Name, string(status))
	stream := w.eventStreamerFn.NewConsumer(
		topic,
		role,
		WithConsumerPollFrequency(pollFrequency),
		WithEventFilter(
			shardFilter(shard, totalShards),
		),
	)

	defer stream.Close()

	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrWorkflowShutdown, "")
		}

		e, ack, err := stream.Recv(ctx)
		if err != nil {
			return err
		}

		r, err := UnmarshalRecord(e.Body)
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

func shardFilter(shard, totalShards int) EventFilter {
	return func(e *Event) bool {
		if totalShards > 1 {
			return e.ID%int64(totalShards) == int64(shard)
		}

		return false
	}
}

func wait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return nil
	}

	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return errors.Wrap(ErrWorkflowShutdown, ctx.Err().Error())
	case <-t.C:
		return nil
	}
}

func awaitWorkflowStatusByForeignID[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], status Status, foreignID, runID string, role string, pollFrequency time.Duration) (*Record[Type, Status], error) {
	topic := Topic(w.Name, string(status))
	stream := w.eventStreamerFn.NewConsumer(
		topic,
		role,
		WithConsumerPollFrequency(pollFrequency),
		WithEventFilter(func(e *Event) bool {
			return e.ForeignID != foreignID
		}),
	)
	defer stream.Close()

	for {
		if ctx.Err() != nil {
			return nil, errors.Wrap(ErrWorkflowShutdown, "")
		}

		e, ack, err := stream.Recv(ctx)
		if err != nil {
			return nil, err
		}

		r, err := UnmarshalRecord(e.Body)
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
			return errors.Wrap(ErrWorkflowShutdown, "")
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

		err = wait(ctx, timeouts.PollingFrequency)
		if err != nil {
			return err
		}
	}
}
