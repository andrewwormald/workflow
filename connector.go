package workflow

import (
	"context"
	"fmt"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
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

	pollFrequency := w.defaultPollingFrequency
	if cc.pollingFrequency.Nanoseconds() != 0 {
		pollFrequency = cc.pollingFrequency
	}

	errBackOff := w.defaultErrBackOff
	if cc.errBackOff.Nanoseconds() != 0 {
		errBackOff = cc.errBackOff
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

	w.run(role, func(ctx context.Context) error {
		return consumeExternalWorkflow[Type, Status](ctx, stream, w, cc.filter, cc.consumer, cc.to)
	}, errBackOff)
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
