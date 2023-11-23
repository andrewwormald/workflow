package workflow

import (
	"context"
	"fmt"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"time"
)

// ConsumerFunc provides a record that is expected to be modified if the data needs to change. If true is returned with
// a nil error then the record, along with its modifications, will be stored. If false is returned with a nil error then
// the record will not be stored and the event will be skipped and move onto the next event. If a non-nil error is
// returned then the consumer will back off and try again until a nil error occurs or the retry max has been reached
// if a Dead Letter Queue has been configured for the workflow.
type ConsumerFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status]) (bool, error)

type consumerConfig[Type any, Status ~string] struct {
	PollingFrequency  time.Duration
	ErrBackOff        time.Duration
	DestinationStatus Status
	Consumer          ConsumerFunc[Type, Status]
	ParallelCount     int
}

func consumer[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], currentStatus Status, p consumerConfig[Type, Status], shard, totalShards int) {
	if w.debugMode {
		log.Info(ctx, "launched consumer", j.MKV{
			"workflow_name": w.Name,
			"from":          currentStatus,
			"to":            p.DestinationStatus,
		})
	}

	role := makeRole(
		w.Name,
		string(currentStatus),
		"to",
		string(p.DestinationStatus),
		"consumer",
		fmt.Sprintf("%v", shard),
		"of",
		fmt.Sprintf("%v", totalShards),
	)

	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "consumer error"))
		}

		w.updateState(role, StateRunning)

		if w.debugMode {
			log.Info(ctx, "consumer obtained role", j.MKV{
				"role": role,
			})
		}

		if ctx.Err() != nil {
			// Gracefully exit when context has been cancelled
			if w.debugMode {
				log.Info(ctx, "shutting down consumer", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
					"shard":              shard,
					"total_shards":       totalShards,
					"role":               role,
				})
			}
			return
		}

		err = runStepConsumerForever[Type, Status](ctx, w, p, currentStatus, role, shard, totalShards)
		if errors.IsAny(err, ErrWorkflowShutdown, context.Canceled) {
			if w.debugMode {
				log.Info(ctx, "shutting down consumer", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
				})
			}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "consumer error"))
		}

		select {
		case <-ctx.Done():
			cancel()
			if w.debugMode {
				log.Info(ctx, "shutting down consumer", j.MKV{
					"workflow_name":      w.Name,
					"current_status":     currentStatus,
					"destination_status": p.DestinationStatus,
				})
			}
			return
		case <-time.After(p.ErrBackOff):
			cancel()
		}
	}
}

func runStepConsumerForever[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], p consumerConfig[Type, Status], status Status, role string, shard, totalShards int) error {
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

		wr, err := UnmarshalRecord(e.Body)
		if err != nil {
			return err
		}

		latest, err := w.recordStore.Latest(ctx, wr.WorkflowName, wr.ForeignID)
		if err != nil {
			return err
		}

		if latest.Status != wr.Status {
			err = ack()
			if err != nil {
				return err
			}
			continue
		}

		if latest.RunID != wr.RunID {
			err = ack()
			if err != nil {
				return err
			}
			continue
		}

		err = consume(ctx, wr, p.Consumer, ack, p.DestinationStatus, w.endPoints, w.eventStreamerFn, w.recordStore)
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

func consume[Type any, Status ~string](ctx context.Context, wr *WireRecord, cf ConsumerFunc[Type, Status], ack Ack, destinationStatus Status, endPoints map[Status]bool, es EventStreamer, rs RecordStore) error {
	var t Type
	err := Unmarshal(wr.Object, &t)
	if err != nil {
		return err
	}

	record := Record[Type, Status]{
		WireRecord: *wr,
		Status:     Status(wr.Status),
		Object:     &t,
	}

	ok, err := cf(ctx, &record)
	if err != nil {
		return errors.Wrap(err, "failed to consume", j.MKV{
			"workflow_name":      wr.WorkflowName,
			"foreign_id":         wr.ForeignID,
			"current_status":     wr.Status,
			"destination_status": destinationStatus,
		})
	}

	if ok {
		b, err := Marshal(&record.Object)
		if err != nil {
			return err
		}

		isEnd := endPoints[destinationStatus]
		wr := &WireRecord{
			RunID:        record.RunID,
			WorkflowName: record.WorkflowName,
			ForeignID:    record.ForeignID,
			Status:       string(destinationStatus),
			IsStart:      false,
			IsEnd:        isEnd,
			Object:       b,
			CreatedAt:    record.CreatedAt,
		}

		err = update(ctx, es, rs, wr)
		if err != nil {
			return err
		}
	}

	return ack()
}
