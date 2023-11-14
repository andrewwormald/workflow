package memstreamer

import (
	"context"
	"github.com/andrewwormald/workflow"
	"sync"
	"time"
)

func New() *StreamConstructor {
	var log []*workflow.WireRecord
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

func (s StreamConstructor) NewConsumer(topic string, name string) workflow.Consumer {
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
	log    *[]*workflow.WireRecord
	offset int
	topic  string
	name   string
}

func (s *Stream) Send(ctx context.Context, r *workflow.WireRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	*s.log = append(*s.log, r)

	return nil
}

func (s *Stream) Recv(ctx context.Context) (*workflow.WireRecord, workflow.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var record *workflow.WireRecord
	for record == nil {
		log := *s.log

		if len(log)-1 < s.offset {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		r := log[s.offset]

		t := workflow.Topic(r.WorkflowName, r.Status)
		if s.topic != t {
			s.offset += 1
			continue
		}

		record = r
	}

	return record, func() error {
		s.offset += 1
		return nil
	}, nil
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
