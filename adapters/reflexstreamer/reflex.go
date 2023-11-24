package reflexstreamer

import (
	"context"
	"database/sql"
	"io"
	"strconv"
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

func (p Producer) Send(ctx context.Context, e *workflow.Event) error {
	tx, err := p.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, err := e.ProtoMarshal()
	if err != nil {
		return err
	}

	eventType, err := TranslateToEventType(p.topic)
	if err != nil {
		return err
	}

	notify, err := p.eventsTable.InsertWithMetadata(ctx, tx, e.ForeignID, eventType, b)
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

		et, err := TranslateToEventType(c.topic)
		if err != nil {
			return nil, nil, err
		}

		if !reflex.IsType(et, reflexEvent.Type) {
			continue
		}

		event, err := workflow.UnmarshalEvent(reflexEvent.MetaData)
		if err != nil {
			return nil, nil, err
		}

		event.ID = reflexEvent.IDInt()
		event.CreatedAt = reflexEvent.Timestamp

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
					"event_id":  event.ID,
					"event_fid": event.ForeignID,
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

var _ workflow.EventStreamer = (*constructor)(nil)
