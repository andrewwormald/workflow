package memrecordstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/luno/jettison/errors"
	"k8s.io/utils/clock"

	"github.com/andrewwormald/workflow"
)

func New(opts ...Option) *Store {
	s := &Store{
		idIncrement:        1,
		timeoutIdIncrement: 1,
		clock:              clock.RealClock{},
		keyIndex:           make(map[string][]*workflow.WireRecord),
		workflowIndex:      make(map[string][]*workflow.WireRecord),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

type Option func(s *Store)

func WithClock(c clock.Clock) Option {
	return func(s *Store) {
		s.clock = c
	}
}

var _ workflow.RecordStore = (*Store)(nil)

type Store struct {
	mu          sync.Mutex
	idIncrement int64

	clock clock.Clock

	keyIndex      map[string][]*workflow.WireRecord
	workflowIndex map[string][]*workflow.WireRecord

	tmu                sync.Mutex
	timeoutIdIncrement int64
	timeouts           []*workflow.Timeout
}

func (s *Store) Store(ctx context.Context, record *workflow.WireRecord, emitter workflow.EventEmitter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record.ID = int64(len(s.workflowIndex[record.WorkflowName])) + 1

	err := emitter(record.ID)
	if err != nil {
		return err
	}

	uk := uniqueKey(record.WorkflowName, record.ForeignID)
	s.keyIndex[uk] = append(s.keyIndex[uk], record)
	s.workflowIndex[record.WorkflowName] = append(s.workflowIndex[record.WorkflowName], record)

	return nil
}

func (s *Store) Latest(ctx context.Context, workflowName, foreignID string) (*workflow.WireRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uk := uniqueKey(workflowName, foreignID)
	records := s.keyIndex[uk]
	if len(records) == 0 {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return records[len(records)-1], nil
}

func uniqueKey(s1, s2 string) string {
	return fmt.Sprintf("%v-%v", s1, s2)
}
