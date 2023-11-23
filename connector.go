package workflow

import (
	"context"
	"fmt"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"time"
)

// ConnectorFilter should return an empty string as the foreignID if the event should be filtered out / skipped, and
// it should be non-empty if event should be processed. The value of foreignID should match the foreignID of your
// workflow.
type ConnectorFilter func(ctx context.Context, e *Event) (foreignID string, err error)

type ConnectorConsumerFunc[Type any, Status ~string] func(ctx context.Context, r *Record[Type, Status], e *Event) (bool, error)

type connectorConfig[Type any, Status ~string] struct {
	workflowName     string
	status           string
	stream           EventStreamer
	filter           ConnectorFilter
	consumer         ConnectorConsumerFunc[Type, Status]
	to               string
	pollingFrequency time.Duration
	errBackOff       time.Duration
	parallelCount    int
}

func connectorConsumer[Type any, Status ~string](ctx context.Context, w *Workflow[Type, Status], cc *connectorConfig[Type, Status], shard, totalShards int) {
	if w.debugMode {
		log.Info(ctx, "launched connector consumer", j.MKV{
			"external_workflow_name": cc.workflowName,
			"from":                   cc.status,
			"workflow_name":          w.Name,
			"to":                     cc.to,
		})
	}

	role := makeRole(
		cc.workflowName,
		cc.status,
		"to",
		w.Name,
		cc.to,
		"connector",
		"consumer",
		fmt.Sprintf("%v", shard),
		"of",
		fmt.Sprintf("%v", totalShards),
	)

	w.updateState(role, StateIdle)
	defer w.updateState(role, StateShutdown)

	pollFrequency := w.defaultPollingFrequency
	if cc.pollingFrequency.Nanoseconds() != 0 {
		pollFrequency = cc.pollingFrequency
	}

	topic := Topic(cc.workflowName, cc.status)
	stream := cc.stream.NewConsumer(
		topic,
		role,
		WithConsumerPollFrequency(pollFrequency),
		WithEventFilter(
			shardFilter(shard, totalShards),
		),
	)
	defer stream.Close()

	for {
		ctx, cancel, err := w.scheduler.Await(ctx, role)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "consumer error"))
		}

		w.updateState(role, StateRunning)

		if w.debugMode {
			log.Info(ctx, "connector consumer obtained role", j.MKV{
				"role": role,
			})
		}

		err = consumeExternalWorkflow[Type, Status](ctx, stream, w, cc.filter, cc.consumer, cc.to)
		if errors.IsAny(err, ErrWorkflowShutdown, context.Canceled) {
			if w.debugMode {
				log.Info(ctx, "shutting down connector consumer", j.MKV{
					"external_workflow_name": cc.workflowName,
					"current_status":         cc.status,
					"workflow_name":          w.Name,
					"destination_status":     cc.status,
				})
			}
		} else if err != nil {
			log.Error(ctx, errors.Wrap(err, "connector consumer error"))
		}

		select {
		case <-ctx.Done():
			cancel()
			if w.debugMode {
				log.Info(ctx, "shutting down connector consumer", j.MKV{
					"external_workflow_name": cc.workflowName,
					"status":                 cc.status,
					"workflow_name":          w.Name,
					"destination_status":     cc.to,
				})
			}
			return
		case <-time.After(cc.errBackOff):
			cancel()
		}
	}
}

func consumeExternalWorkflow[Type any, Status ~string](ctx context.Context, stream Consumer, w *Workflow[Type, Status], filter ConnectorFilter, consumerFunc ConnectorConsumerFunc[Type, Status], to string) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrWorkflowShutdown, "")
		}

		e, ack, err := stream.Recv(ctx)
		if err != nil {
			return err
		}

		foreignID, err := filter(ctx, e)
		if err != nil {
			return err
		}

		if foreignID == "" {
			err = ack()
			if err != nil {
				return err
			}

			continue
		}

		latest, err := w.recordStore.Latest(ctx, w.Name, foreignID)
		if err != nil {
			return err
		}

		var t Type
		err = Unmarshal(latest.Object, &t)
		if err != nil {
			return err
		}

		record := Record[Type, Status]{
			WireRecord: *latest,
			Status:     Status(latest.Status),
			Object:     &t,
		}

		ok, err := consumerFunc(ctx, &record, e)
		if err != nil {
			return errors.Wrap(err, "failed to consume - connector consumer", j.MKV{
				"workflow_name":      latest.WorkflowName,
				"foreign_id":         latest.ForeignID,
				"current_status":     latest.Status,
				"destination_status": to,
			})
		}

		if ok {
			b, err := Marshal(&record.Object)
			if err != nil {
				return err
			}

			isEnd := w.endPoints[Status(to)]
			wr := &WireRecord{
				RunID:        record.RunID,
				WorkflowName: record.WorkflowName,
				ForeignID:    record.ForeignID,
				Status:       to,
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

		return ack()
	}
}
