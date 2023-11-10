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

func (s StreamConstructor) New(workflowName string, status string) workflow.EventStreamer {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()

	return &Stream{
		log:          s.stream.log,
		workflowName: workflowName,
		status:       status,
	}
}

var _ workflow.EventStreamerConstructor = (*StreamConstructor)(nil)

type Stream struct {
	mu           sync.Mutex
	log          *[]*workflow.WireRecord
	offset       int
	workflowName string
	status       string
}

func (s *Stream) Send(ctx context.Context, r *workflow.WireRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	*s.log = append(*s.log, r)

	return nil
}

func (s *Stream) Recv(ctx context.Context) (*workflow.WireRecord, error) {
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
		s.offset++

		if r.WorkflowName != s.workflowName {
			continue
		}

		if r.Status != s.status {
			continue
		}

		record = r
	}

	return record, nil
}

func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = nil
	s.offset = 0
	return nil
}

var _ workflow.EventStreamer = (*Stream)(nil)
