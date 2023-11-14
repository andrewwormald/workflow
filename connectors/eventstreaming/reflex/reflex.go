package reflex

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
	"io"
	"strings"
	"time"
)

func New(writer, reader *sql.DB, table *rsql.EventsTable, cursorStore reflex.CursorStore, opts ...reflex.StreamOption) workflow.EventStreamerConstructor {
	return &constructor{
		writer:      writer,
		reader:      reader,
		stream:      table.ToStream(reader, opts...),
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

func (p Producer) Send(ctx context.Context, r *workflow.WireRecord) error {
	tx, err := p.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, err := r.ProtoMarshal()
	if err != nil {
		return err
	}

	eventType, err := TranslateToEventType(p.topic)
	if err != nil {
		return err
	}

	notify, err := p.eventsTable.InsertWithMetadata(ctx, tx, r.ForeignID, eventType, b)
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

func (c constructor) NewConsumer(topic string, name string) workflow.Consumer {
	return &Consumer{
		topic:       topic,
		name:        name,
		cursor:      c.cursorStore,
		reader:      c.reader,
		stream:      c.stream,
		pollBackOff: time.Millisecond * 200,
	}
}

type Consumer struct {
	topic       string
	name        string
	cursor      reflex.CursorStore
	reader      *sql.DB
	stream      reflex.StreamFunc
	pollBackOff time.Duration
}

func (c Consumer) Recv(ctx context.Context) (*workflow.WireRecord, workflow.Ack, error) {
	cursor, err := c.cursor.GetCursor(ctx, c.name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to collect cursor")
	}

	cl, err := c.stream(ctx, cursor)
	if err != nil {
		return nil, nil, err
	}

	for ctx.Err() == nil {
		event, err := cl.Recv()
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

		if !reflex.IsType(et, event.Type) {
			continue
		}

		wr, err := workflow.UnmarshalRecord(event.MetaData)
		if err != nil {
			return nil, nil, err
		}

		return wr, func() error {
			// Increment cursor for consumer only if ack function is called.
			if err := c.cursor.SetCursor(ctx, c.name, event.ID); err != nil {
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

func wait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return nil
	}

	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		fmt.Println("context was cancelled")
		return errors.Wrap(workflow.ErrStreamingClosed, ctx.Err().Error())
	case <-t.C:
		return nil
	}
}

func cursorKey(workflowName string, status string) string {
	return strings.Join([]string{workflowName, status}, "-")
}

var _ workflow.EventStreamerConstructor = (*constructor)(nil)
