package memstreamer

import (
	"context"
	"sync"
	"time"

	"github.com/andrewwormald/workflow"
)

func New() *StreamConstructor {
	var log []*workflow.WireRecord
	return &StreamConstructor{
		stream: &Stream{
			mu:  &sync.Mutex{},
			log: &log,
		},
	}
}

type StreamConstructor struct {
	stream *Stream
}

func (s StreamConstructor) NewProducer(topic string) workflow.Producer {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()

	return &Stream{
		mu:    s.stream.mu,
		log:   s.stream.log,
		topic: topic,
	}
}

func (s StreamConstructor) NewConsumer(topic string, name string, opts ...workflow.ConsumerOption) workflow.Consumer {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()

	return &Stream{
		mu:    s.stream.mu,
		log:   s.stream.log,
		topic: topic,
		name:  name,
	}
}

var _ workflow.EventStreamer = (*StreamConstructor)(nil)

type Stream struct {
	mu     *sync.Mutex
	log    *[]*workflow.WireRecord
	offset int
	topic  string
	name   string
}

func (s *Stream) Send(ctx context.Context, wr *workflow.WireRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Storing in the header to filter on recv
	wr.Headers["topic"] = s.topic

	*s.log = append(*s.log, wr)

	return nil
}

func (s *Stream) Recv(ctx context.Context) (*workflow.Event, workflow.Ack, error) {
	for ctx.Err() == nil {
		s.mu.Lock()
		log := *s.log
		s.mu.Unlock()

		if len(log)-1 < s.offset {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		e := log[s.offset]

		if s.topic != e.Headers["topic"] {
			s.offset += 1
			continue
		}

		wr := log[s.offset]
		event := &workflow.Event{
			ID:        int64(s.offset),
			CreatedAt: wr.CreatedAt,
			Record:    log[s.offset],
		}
		return event, func() error {
			s.offset += 1
			return nil
		}, nil
	}

	return nil, nil, ctx.Err()
}

func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = nil
	s.offset = 0
	return nil
}

var (
	_ workflow.Producer = (*Stream)(nil)
	_ workflow.Consumer = (*Stream)(nil)
)
