package memrecordstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/luno/jettison/errors"
	"k8s.io/utils/clock"

	"github.com/andrewwormald/workflow"
)

func New() *Store {
	s := &Store{
		idIncrement:        1,
		timeoutIdIncrement: 1,
		clock:              clock.RealClock{},
		keyIndex:           make(map[string][]*workflow.WireRecord),
		store:              make(map[int64]*workflow.WireRecord),
	}

	return s
}

var _ workflow.RecordStore = (*Store)(nil)

type Store struct {
	mu          sync.Mutex
	idIncrement int64

	clock clock.Clock

	keyIndex map[string][]*workflow.WireRecord
	store    map[int64]*workflow.WireRecord

	tmu                sync.Mutex
	timeoutIdIncrement int64
	timeouts           []*workflow.Timeout
}

func (s *Store) Lookup(ctx context.Context, id int64) (*workflow.WireRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.store[id]
	if !ok {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return r, nil
}

func (s *Store) Store(ctx context.Context, record *workflow.WireRecord, emitter workflow.EventEmitter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.idIncrement++

	record.ID = s.idIncrement

	err := emitter(record.ID)
	if err != nil {
		return err
	}

	uk := uniqueKey(record.WorkflowName, record.ForeignID)
	s.keyIndex[uk] = append(s.keyIndex[uk], record)
	s.store[record.ID] = record

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
