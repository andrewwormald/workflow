package memstore

import (
	"context"
	"fmt"
	"k8s.io/utils/clock"
	"sync"
	"time"

	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

func New(opts ...Option) *Store {
	s := &Store{
		idIncrement:        1,
		timeoutIdIncrement: 1,
		clock:              clock.RealClock{},
		keyIndex:           make(map[string][]*workflow.Record),
		workflowIndex:      make(map[string][]*workflow.Record),
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

var _ workflow.Store = (*Store)(nil)

type Store struct {
	mu          sync.Mutex
	idIncrement int64

	clock clock.Clock

	keyIndex      map[string][]*workflow.Record
	workflowIndex map[string][]*workflow.Record

	tmu                sync.Mutex
	timeoutIdIncrement int64
	timeouts           []*workflow.Timeout
}

func (s *Store) Store(ctx context.Context, key workflow.Key, status string, object []byte, isStart, isEnd bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rPointer := &workflow.Record{
		ID:           s.idIncrement,
		WorkflowName: key.WorkflowName,
		ForeignID:    key.ForeignID,
		RunID:        key.RunID,
		Status:       status,
		Object:       object,
		IsStart:      isStart,
		IsEnd:        isEnd,
		CreatedAt:    s.clock.Now(),
	}

	uk := uniqueKey(key.WorkflowName, key.ForeignID, key.RunID)
	s.keyIndex[uk] = append(s.keyIndex[uk], rPointer)
	s.workflowIndex[key.WorkflowName] = append(s.workflowIndex[key.WorkflowName], rPointer)

	s.incrementID()

	return nil
}

func (s *Store) LookupLatest(ctx context.Context, key workflow.Key) (*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uk := uniqueKey(key.WorkflowName, key.ForeignID, key.RunID)
	records := s.keyIndex[uk]
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

func (s *Store) Find(ctx context.Context, key workflow.Key, status string) (*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.keyIndex[uniqueKey(key.WorkflowName, key.ForeignID, key.RunID)] {
		if record.Status != status {
			continue
		}

		return record, nil
	}

	return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
}

func (s *Store) CreateTimeout(ctx context.Context, key workflow.Key, status string, expireAt time.Time) error {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	s.timeouts = append(s.timeouts, &workflow.Timeout{
		ID:           s.timeoutIdIncrement,
		WorkflowName: key.WorkflowName,
		ForeignID:    key.ForeignID,
		Status:       status,
		RunID:        key.RunID,
		ExpireAt:     expireAt,
		CreatedAt:    s.clock.Now(),
	})
	s.timeoutIdIncrement++

	return nil
}

func (s *Store) CompleteTimeout(ctx context.Context, id int64) error {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	for i, timeout := range s.timeouts {
		if timeout.ID != id {
			continue
		}

		s.timeouts[i].Completed = true
		break
	}

	return nil
}

func (s *Store) CancelTimeout(ctx context.Context, id int64) error {
	var index int
	for i, timeout := range s.timeouts {
		if timeout.ID != id {
			continue
		}

		index = i
		break
	}

	left := s.timeouts[:index]
	right := s.timeouts[index+1 : len(s.timeouts)]
	s.timeouts = append(left, right...)
	return nil
}

func (s *Store) ListValidTimeouts(ctx context.Context, workflowName string, status string, now time.Time) ([]workflow.Timeout, error) {
	var valid []workflow.Timeout
	for _, timeout := range s.timeouts {
		if timeout.WorkflowName != workflowName {
			continue
		}

		if timeout.Status != status {
			continue
		}

		if timeout.Completed {
			continue
		}

		if timeout.ExpireAt.After(now) {
			continue
		}

		valid = append(valid, *timeout)
	}

	return valid, nil
}

func (s *Store) LastRunID(ctx context.Context, workflowName string, foreignID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.workflowIndex[workflowName]
	for i := len(entries) - 1; i >= 0; i-- {
		record := entries[i]
		if record.ForeignID != foreignID {
			continue
		}

		return record.RunID, nil
	}

	return "", errors.Wrap(workflow.ErrRunIDNotFound, "")
}

func (s *Store) LastRecordForWorkflow(ctx context.Context, workflowName string) (*workflow.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.workflowIndex[workflowName][len(s.workflowIndex[workflowName])-1], nil
}

func (s *Store) WorkflowBatch(ctx context.Context, workflowName string, fromID int64, size int) ([]*workflow.Record, error) {
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

		batch = append(batch, record)
	}

	return batch, nil
}

func (s *Store) incrementID() {
	s.idIncrement++
}

func uniqueKey(s1, s2, s3 string) string {
	return fmt.Sprintf("%v-%v-%v", s1, s2, s3)
}
