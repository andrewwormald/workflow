package sqlstore

import (
	"context"
	"database/sql"

	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

type SQLStore struct {
	writer *sql.DB
	reader *sql.DB

	recordTableName    string
	recordCols         string
	recordSelectPrefix string

	timeoutTableName    string
	timeoutCols         string
	timeoutSelectPrefix string
}

func New(recordTableName string, timeoutTableName string, writer *sql.DB, reader *sql.DB) *SQLStore {
	e := &SQLStore{
		recordTableName:  recordTableName,
		timeoutTableName: timeoutTableName,
		writer:           writer,
		reader:           reader,
	}

	e.recordCols = " `id`, `workflow_name`, `foreign_id`, `run_id`, `status`, `object`, `is_start`, `is_end`, `created_at` "
	e.recordSelectPrefix = " select " + e.recordCols + " from " + e.recordTableName + " where "

	e.timeoutCols = " `id`, `workflow_name`, `foreign_id`, `run_id`, `status`, `completed`, `expire_at`, `created_at` "
	e.timeoutSelectPrefix = " select " + e.timeoutCols + " from " + e.timeoutTableName + " where "

	return e
}

var _ workflow.RecordStore = (*SQLStore)(nil)

func (s *SQLStore) Store(ctx context.Context, r *workflow.WireRecord) error {
	_, err := s.create(ctx, r.WorkflowName, r.ForeignID, r.RunID, r.Status, r.Object, r.IsStart, r.IsEnd)
	return err
}

func (s *SQLStore) Latest(ctx context.Context, workflowName, foreignID string) (*workflow.WireRecord, error) {
	ls, err := s.listWhere(ctx, s.reader, "workflow_name=? and foreign_id=? order by id desc limit 1", workflowName, foreignID)
	if err != nil {
		return nil, err
	}

	if len(ls) < 1 {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return ls[0], nil
}
