package workflow

import (
	"context"
	"time"
)

type RecordStore interface {
	Store(ctx context.Context, record *WireRecord) error
	Latest(ctx context.Context, workflowName, foreignID string) (*WireRecord, error)
}

type TimeoutStore interface {
	Create(ctx context.Context, workflowName, foreignID, runID string, status int, expireAt time.Time) error
	Complete(ctx context.Context, workflowName, foreignID, runID string, status int) error
	Cancel(ctx context.Context, workflowName, foreignID, runID string, status int) error
	List(ctx context.Context, workflowName string) ([]Timeout, error)
	ListValid(ctx context.Context, workflowName string, status int, now time.Time) ([]Timeout, error)
}
