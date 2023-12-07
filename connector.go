package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

// ConnectorFilter should return an empty string as the foreignID if the event should be filtered out / skipped, and
// it should be non-empty if event should be processed. The value of foreignID should match the foreignID of your
// workflow.
type ConnectorFilter func(ctx context.Context, e *Event) (foreignID string, err error)

type ConnectorConsumerFunc[Type any, Status StatusType] func(ctx context.Context, r *Record[Type, Status], e *Event) (bool, error)

type ConnectionDetails struct {
	WorkflowName string
	Status       int
	Stream       EventStreamer
}

type connectorConfig[Type any, Status StatusType] struct {
	workflowName     string
	status           int
	stream           EventStreamer
	filter           ConnectorFilter
	consumer         ConnectorConsumerFunc[Type, Status]
	from             Status
	to               Status
	pollingFrequency time.Duration
	errBackOff       time.Duration
	parallelCount    int
}

func connectorConsumer[Type any, Status StatusType](w *Workflow[Type, Status], cc *connectorConfig[Type, Status], shard, totalShards int) {
	role := makeRole(
		cc.workflowName,
		fmt.Sprintf("%v", cc.status),
		"connection",
		w.Name,
		fmt.Sprintf("%v", int(cc.from)),
		"to",
		fmt.Sprintf("%v", int(cc.to)),
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
		return consumeExternalWorkflow[Type, Status](ctx, stream, w, cc.workflowName, cc.status, cc.filter, cc.consumer, cc.to)
	}, errBackOff)
}

func consumeExternalWorkflow[Type any, Status StatusType](ctx context.Context, stream Consumer, w *Workflow[Type, Status], externalWorkflowName string, status int, filter ConnectorFilter, consumerFunc ConnectorConsumerFunc[Type, Status], to Status) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ErrWorkflowShutdown, "")
		}

		e, ack, err := stream.Recv(ctx)
		if err != nil {
			return err
		}

		if e.Headers[HeaderWorkflowName] != externalWorkflowName {
			err = ack()
			if err != nil {
				return err
			}

			continue
		}

		if e.Type != status {
			err = ack()
			if err != nil {
				return err
			}

			continue
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

			isEnd := w.endPoints[to]
			wr := &WireRecord{
				RunID:        record.RunID,
				WorkflowName: record.WorkflowName,
				ForeignID:    record.ForeignID,
				Status:       int(to),
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
