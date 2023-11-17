package memstreamer_test

import (
	"context"
	connector "github.com/andrewwormald/workflow/connectors/testing"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/connectors/eventstreaming/memstreamer"
)

func TestMemSteamer(t *testing.T) {
	stream := memstreamer.New()
	workflowName := "example-workflow"
	status := "Started"

	ctx := context.TODO()
	eventRecords := []workflow.WireRecord{
		{
			WorkflowName: workflowName,
			ForeignID:    "example_foreign_id_1",
			RunID:        "26736768756872257245",
			Status:       status,
			IsStart:      true,
			IsEnd:        false,
			CreatedAt:    time.Time{},
		},
		{
			WorkflowName: workflowName,
			ForeignID:    "example_foreign_id_2",
			RunID:        "34645763456745672456",
			Status:       status,
			IsStart:      true,
			IsEnd:        false,
			CreatedAt:    time.Time{},
		},
		{
			WorkflowName: workflowName,
			ForeignID:    "example_foreign_id_3",
			RunID:        "34345345345345345345",
			Status:       status,
			IsStart:      true,
			IsEnd:        false,
			CreatedAt:    time.Time{},
		},
	}

	for _, record := range eventRecords {
		body, err := record.ProtoMarshal()
		jtest.RequireNil(t, err)

		e := workflow.Event{
			ForeignID: record.ForeignID,
			Body:      body,
			Headers:   make(map[string]string),
		}
		topic := workflow.Topic(workflowName, status)
		err = stream.NewProducer(topic).Send(ctx, &e)
		jtest.RequireNil(t, err)
	}

	topic := workflow.Topic(workflowName, status)
	consumer := stream.NewConsumer(topic, status)
	for _, expected := range eventRecords {
		event, ack, err := consumer.Recv(ctx)
		jtest.RequireNil(t, err)

		actual, err := workflow.UnmarshalRecord(event.Body)
		jtest.RequireNil(t, err)

		require.Equal(t, expected, *actual)

		err = ack()
		jtest.RequireNil(t, err)
	}
}

func TestStreamer(t *testing.T) {
	constructor := memstreamer.New()
	connector.TestStreamer(t, constructor)
}
