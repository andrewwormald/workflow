package workflowstreamer

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"

	"github.com/andrewwormald/workflow"
)

var _ reflex.StreamClient = (*stream)(nil)

type RecordType int

var (
	RecordTypeUnknown   RecordType = 0
	RecordTypeStarted   RecordType = 1
	RecordTypeNormal    RecordType = 2
	RecordTypeCompleted RecordType = 3
)

func (rt RecordType) ReflexType() int {
	return int(rt)
}

func NewReflexStreamer(store workflow.Store, workflowName string) reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		return newStream(ctx, store, workflowName, after, opts...), nil
	}
}

func newStream(ctx context.Context, st workflow.Store, workflowName string, after string, opts ...reflex.StreamOption) *stream {
	var so reflex.StreamOptions

	for _, opt := range opts {
		opt(&so)
	}

	return &stream{
		ctx:          ctx,
		workflowName: workflowName,
		store:        st,
		buf:          new(buffer),
		options:      so,
		prev: reflex.Event{
			ID: after,
		},
	}
}

type stream struct {
	ctx          context.Context
	workflowName string
	store        workflow.Store

	prev reflex.Event
	buf  *buffer

	options reflex.StreamOptions
}

func (s *stream) Recv() (*reflex.Event, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	if s.prev.IDInt() == 0 && s.options.StreamFromHead {
		e, err := s.store.LastRecordForWorkflow(s.ctx, s.workflowName)
		if errors.Is(err, ErrEntryNotFound) {
			// NoReturnErr: Handle with zero value if there are no entries to stream
			e = &workflow.Record{}
		} else if err != nil {
			return nil, err
		}

		oneAwayFromLatest := e.ID - 1
		s.prev.ID = strconv.FormatInt(oneAwayFromLatest, 10)
	}

	for s.buf.IsEmpty() {
		b, err := nextBatch(s.ctx, s.store, s.workflowName, s.prev.IDInt())
		if err != nil {
			return nil, err
		}

		// Set the new buffer for consumption below.
		s.buf.Set(b)

		// No need to backoff if we have filled the buffer with new events.
		if !s.buf.IsEmpty() {
			break
		}

		// No new events will mean the stream is at the head
		if s.options.StreamToHead {
			return nil, errors.Wrap(reflex.ErrHeadReached, "")
		}

		err = waitUntil(s.ctx, 5*time.Second)
		if err != nil {
			return nil, err
		}
	}

	next, err := s.buf.Pop()
	if err != nil {
		return nil, err
	}

	// Ensure we keep a current position
	s.prev = *next

	return next, nil
}

func waitUntil(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func nextBatch(ctx context.Context, s workflow.Store, workflowName string, since int64) ([]*reflex.Event, error) {
	rs, err := s.WorkflowBatch(ctx, workflowName, since, 1000)
	if err != nil {
		return nil, err
	}

	var res []*reflex.Event
	for _, r := range rs {
		pb, err := r.ProtoMarshal()
		if err != nil {
			return nil, errors.Wrap(err, "failed to proto marshal entry")
		}

		var rt RecordType
		switch true {
		case r.IsStart:
			rt = RecordTypeStarted
		case r.IsEnd:
			rt = RecordTypeCompleted
		default:
			rt = RecordTypeNormal
		}

		res = append(res, &reflex.Event{
			ID:        strconv.FormatInt(r.ID, 10),
			Type:      rt,
			ForeignID: r.ForeignID,
			Timestamp: r.CreatedAt,
			MetaData:  pb,
		})
	}

	return res, nil
}

type buffer struct {
	mu  sync.Mutex
	buf []*reflex.Event
}

func (b *buffer) IsEmpty() bool {
	return len(b.buf) == 0
}

func (b *buffer) Set(es []*reflex.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buf = es
}

var errBufEmpty = errors.New("buffer is empty")

func (b *buffer) Pop() (*reflex.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.IsEmpty() {
		return nil, errors.Wrap(errBufEmpty, "")
	}

	e := b.buf[0]
	b.buf = b.buf[1:]
	return e, nil
}
