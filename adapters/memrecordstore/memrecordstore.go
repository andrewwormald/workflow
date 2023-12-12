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
		keyIndex:         make(map[string]*workflow.WireRecord),
		store:            make(map[int64]*workflow.WireRecord),
		snapshots:        make(map[string][]*workflow.WireRecord),
		snapshotsOffsets: make(map[string]int),
	}

	return s
}

var _ workflow.RecordStore = (*Store)(nil)

type Store struct {
	mu          sync.Mutex
	idIncrement int64

	clock clock.Clock

	keyIndex         map[string]*workflow.WireRecord
	store            map[int64]*workflow.WireRecord
	snapshots        map[string][]*workflow.WireRecord
	snapshotsOffsets map[string]int
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

	if record.ID == 0 {
		s.idIncrement++
		record.ID = s.idIncrement
	}

	err := emitter(record.ID)
	if err != nil {
		return err
	}

	uk := uniqueKey(record.WorkflowName, record.ForeignID)
	s.keyIndex[uk] = record
	s.store[record.ID] = record

	snapshotKey := fmt.Sprintf("%v-%v-%v", record.WorkflowName, record.ForeignID, record.RunID)
	s.snapshots[snapshotKey] = append(s.snapshots[snapshotKey], record)

	return nil
}

func (s *Store) Latest(ctx context.Context, workflowName, foreignID string) (*workflow.WireRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uk := uniqueKey(workflowName, foreignID)
	record, ok := s.keyIndex[uk]
	if !ok {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return record, nil
}

func (s *Store) Snapshots(workflowName, foreignID, runID string) []*workflow.WireRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%v-%v-%v", workflowName, foreignID, runID)
	return s.snapshots[key]
}

func (s *Store) SetSnapshotOffset(workflowName, foreignID, runID string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%v-%v-%v", workflowName, foreignID, runID)
	s.snapshotsOffsets[key] = offset
}

func (s *Store) SnapshotOffset(workflowName, foreignID, runID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%v-%v-%v", workflowName, foreignID, runID)
	return s.snapshotsOffsets[key]
}

func uniqueKey(s1, s2 string) string {
	return fmt.Sprintf("%v-%v", s1, s2)
}
