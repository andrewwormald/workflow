package sqlstore

import (
	"context"
	"database/sql"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/errors"
)

type SQLStore struct {
	writer *sql.DB
	reader *sql.DB

	name         string
	cols         string
	selectPrefix string
}

func New(name string, writer *sql.DB, reader *sql.DB) *SQLStore {
	e := &SQLStore{
		name:   name,
		writer: writer,
		reader: reader,
	}

	e.cols = " `id`, `workflow_name`, `foreign_id`, `status`, `object`, `created_at` "
	e.selectPrefix = " select " + e.cols + " from " + name + " where "

	return e
}

var _ workflow.Store = (*SQLStore)(nil)

func (s *SQLStore) LookupLatest(ctx context.Context, workflowName string, foreignID string) (*workflow.Record, error) {
	ls, err := s.listWhere(ctx, s.reader, "workflow_name=? and foreign_id=? order by id desc limit 1", workflowName, foreignID)
	if err != nil {
		return nil, err
	}

	if len(ls) < 1 {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	}

	return ls[0], nil
}

func (s *SQLStore) Store(ctx context.Context, workflowName string, foreignID string, status string, object []byte) error {
	_, err := s.create(ctx, workflowName, foreignID, status, object)
	return err
}

func (s *SQLStore) Batch(ctx context.Context, workflowName string, status string, fromID int64, size int) ([]*workflow.Record, error) {
	return s.listWhere(ctx, s.reader, "workflow_name=? and status=? and id > ? order by id asc limit ?", workflowName, status, fromID, size)
}

func (s *SQLStore) Find(ctx context.Context, workflowName string, foreignID string, status string) (*workflow.Record, error) {
	return s.lookupWhere(ctx, s.reader, "workflow_name=? and foreign_id=? and status=?", workflowName, foreignID, status)
}
