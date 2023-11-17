package memstreamer

import (
	"context"
	"github.com/andrewwormald/workflow"
	"sync"
	"time"
)

func New() *StreamConstructor {
	var log []*workflow.Event
	return &StreamConstructor{
		stream: &Stream{
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
		log:   s.stream.log,
		topic: topic,
	}
}

func (s StreamConstructor) NewConsumer(topic string, name string, opts ...workflow.ConsumerOption) workflow.Consumer {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()

	return &Stream{
		log:   s.stream.log,
		topic: topic,
		name:  name,
	}
}

var _ workflow.EventStreamerConstructor = (*StreamConstructor)(nil)

type Stream struct {
	mu     sync.Mutex
	log    *[]*workflow.Event
	offset int
	topic  string
	name   string
}

func (s *Stream) Send(ctx context.Context, e *workflow.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Storing in the header to filter on recv
	e.Headers["topic"] = s.topic

	*s.log = append(*s.log, e)

	return nil
}

func (s *Stream) Recv(ctx context.Context) (*workflow.Event, workflow.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for ctx.Err() == nil {
		log := *s.log

		if len(log)-1 < s.offset {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		e := log[s.offset]

		if s.topic != e.Headers["topic"] {
			s.offset += 1
			continue
		}

		return log[s.offset], func() error {
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

var _ workflow.Producer = (*Stream)(nil)
var _ workflow.Consumer = (*Stream)(nil)
