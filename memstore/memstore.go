package memstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

func New() *Store {
	return &Store{
		idIncrement:    1,
		foreignIDIndex: make(map[string][]*workflow.Record),
		workflowIndex:  make(map[string][]*workflow.Record),
	}
}

var _ workflow.Store = (*Store)(nil)

type Store struct {
	mu             sync.Mutex
	idIncrement    int64
	foreignIDIndex map[string][]*workflow.Record
	workflowIndex  map[string][]*workflow.Record
}

func (s *Store) Store(ctx context.Context, workflowName string, foreignID string, status string, object []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rPointer := &workflow.Record{
		ID:           s.idIncrement,
		WorkflowName: workflowName,
		ForeignID:    foreignID,
		Status:       status,
		Object:       object,
	}

	uk := uniqueKey(workflowName, foreignID)
	s.foreignIDIndex[uk] = append(s.foreignIDIndex[uk], rPointer)
	s.workflowIndex[workflowName] = append(s.workflowIndex[workflowName], rPointer)

	s.incrementID()

	return nil
}

func (s *Store) LookupLatest(ctx context.Context, workflowName string, foreignID string) (*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uk := uniqueKey(workflowName, foreignID)
	records := s.foreignIDIndex[uk]
	if len(records) == 0 {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return records[len(records)-1], nil
}

func (s *Store) Batch(ctx context.Context, workflowName string, status string, fromID int64, size int) ([]*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var batch []*workflow.Record
	for _, record := range s.workflowIndex[workflowName] {
		if len(batch) >= size {
			break
		}

		if record.ID <= fromID {
			continue
		}

		if record.Status != status {
			continue
		}

		batch = append(batch, record)
	}

	return batch, nil
}

func (s *Store) Find(ctx context.Context, workflowName string, foreignID string, status string) (*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.foreignIDIndex[uniqueKey(workflowName, foreignID)] {
		if record.Status != status {
			continue
		}

		return record, nil
	}

	return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
}

func (s *Store) incrementID() {
	s.idIncrement++
}

func uniqueKey(s1, s2 string) string {
	return fmt.Sprintf("%v-%v", s1, s2)
}
