package reflexstreamer

import (
	"context"
	"database/sql"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"

	"github.com/andrewwormald/workflow"
)

func New(writer, reader *sql.DB, table *rsql.EventsTable, cursorStore reflex.CursorStore) workflow.EventStreamer {
	return &constructor{
		writer:      writer,
		reader:      reader,
		eventsTable: table,
		cursorStore: cursorStore,
	}
}

type constructor struct {
	writer      *sql.DB
	reader      *sql.DB
	stream      reflex.StreamFunc
	eventsTable *rsql.EventsTable
	cursorStore reflex.CursorStore
}

func (c constructor) NewProducer(topic string) workflow.Producer {
	return &Producer{
		topic:       topic,
		writer:      c.writer,
		eventsTable: c.eventsTable,
	}
}

type Producer struct {
	topic       string
	writer      *sql.DB
	eventsTable *rsql.EventsTable
}

func (p Producer) Send(ctx context.Context, wr *workflow.WireRecord) error {
	tx, err := p.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, err := wr.ProtoMarshal()
	if err != nil {
		return err
	}

	foreignID := makeForeignID(wr)
	notify, err := p.eventsTable.InsertWithMetadata(ctx, tx, foreignID, EventType(wr.Status), b)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	notify()

	return nil
}

func (p Producer) Close() error {
	return nil
}

func (c constructor) NewConsumer(topic string, name string, opts ...workflow.ConsumerOption) workflow.Consumer {
	var copts workflow.ConsumerOptions
	for _, opt := range opts {
		opt(&copts)
	}

	pollFrequency := time.Millisecond * 50
	if copts.PollFrequency.Nanoseconds() != 0 {
		pollFrequency = copts.PollFrequency
	}

	table := c.eventsTable.Clone(rsql.WithEventsBackoff(pollFrequency))
	return &Consumer{
		topic:   topic,
		name:    name,
		cursor:  c.cursorStore,
		reader:  c.reader,
		stream:  table.ToStream(c.reader),
		options: copts,
	}
}

type Consumer struct {
	topic   string
	name    string
	cursor  reflex.CursorStore
	reader  *sql.DB
	stream  reflex.StreamFunc
	options workflow.ConsumerOptions
}

func (c Consumer) Recv(ctx context.Context) (*workflow.Event, workflow.Ack, error) {
	cursor, err := c.cursor.GetCursor(ctx, c.name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to collect cursor")
	}

	cl, err := c.stream(ctx, cursor)
	if err != nil {
		return nil, nil, err
	}

	for ctx.Err() == nil {
		reflexEvent, err := cl.Recv()
		if err != nil {
			return nil, nil, err
		}

		if closer, ok := cl.(io.Closer); ok {
			defer closer.Close()
		}

		eventWorkflowName, _ := splitForeignID(reflexEvent)
		if err != nil {
			return nil, nil, err
		}

		topicWorkflowName, status, err := workflow.ParseTopic(c.topic)
		if err != nil {
			return nil, nil, err
		}

		if eventWorkflowName != topicWorkflowName {
			continue
		}

		if !reflex.IsType(EventType(status), reflexEvent.Type) {
			continue
		}

		wr, err := workflow.UnmarshalRecord(reflexEvent.MetaData)
		if err != nil {
			return nil, nil, err
		}
		event := &workflow.Event{
			ID:        reflexEvent.IDInt(),
			CreatedAt: reflexEvent.Timestamp,
			Record:    wr,
		}

		// Filter out unwanted events
		if skip := c.options.EventFilter(event); skip {
			continue
		}

		return event, func() error {
			// Increment cursor for consumer only if ack function is called.
			eventID := strconv.FormatInt(event.ID, 10)
			if err := c.cursor.SetCursor(ctx, c.name, eventID); err != nil {
				return errors.Wrap(err, "failed to set cursor", j.MKV{
					"consumer":  c.name,
					"event_id":  reflexEvent.ID,
					"event_fid": reflexEvent.ForeignID,
				})
			}

			return nil
		}, nil
	}

	// If the loop breaks then ctx.Err is non-nil
	return nil, nil, ctx.Err()
}

func (c Consumer) Close() error {
	// Provide new context for flushing of cursor values to underlying store
	err := c.cursor.Flush(context.Background())
	if err != nil {
		return err
	}

	return nil
}

const separator = "-"

func makeForeignID(e *workflow.WireRecord) string {
	return strings.Join([]string{e.WorkflowName, e.ForeignID}, separator)
}

func splitForeignID(e *reflex.Event) (workflowName, foreignID string) {
	parts := strings.Split(e.ForeignID, separator)
	return parts[0], parts[1]
}

var _ workflow.EventStreamer = (*constructor)(nil)
