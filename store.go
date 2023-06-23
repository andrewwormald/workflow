package workflow

import (
	"context"
	"time"
)

type Store interface {
	LookupLatest(ctx context.Context, key Key) (*Record, error)
	Store(ctx context.Context, key Key, status string, object []byte, isStart, isEnd bool) error
	Batch(ctx context.Context, workflowName string, status string, fromID int64, size int) ([]*Record, error)
	Find(ctx context.Context, key Key, status string) (*Record, error)
	LastRunID(ctx context.Context, workflowName string, foreignID string) (string, error)
	LastRecordForWorkflow(ctx context.Context, workflowName string) (*Record, error)
	WorkflowBatch(ctx context.Context, workflowName string, fromID int64, size int) ([]*Record, error)

	CreateTimeout(ctx context.Context, key Key, status string, expireAt time.Time) error
	CompleteTimeout(ctx context.Context, id int64) error
	CancelTimeout(ctx context.Context, id int64) error
	ListValidTimeouts(ctx context.Context, workflowName string, status string, now time.Time) ([]Timeout, error)
}

type Key struct {
	WorkflowName string
	ForeignID    string
	RunID        string
}

func MakeKey(workflowName string, foreignID string, runID string) Key {
	return Key{
		WorkflowName: workflowName,
		ForeignID:    foreignID,
		RunID:        runID,
	}
}
