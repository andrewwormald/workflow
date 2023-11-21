package sqlstore

//
//import (
//	"context"
//	"database/sql"
//	"github.com/andrewwormald/workflow"
//	"github.com/luno/jettison/errors"
//	"github.com/luno/jettison/j"
//	"time"
//)
//
//type SQLStore struct {
//	writer *sql.DB
//	reader *sql.DB
//
//	recordTableName    string
//	recordCols         string
//	recordSelectPrefix string
//
//	timeoutTableName    string
//	timeoutCols         string
//	timeoutSelectPrefix string
//}
//
//func New(recordTableName string, timeoutTableName string, writer *sql.DB, reader *sql.DB) *SQLStore {
//	e := &SQLStore{
//		recordTableName:  recordTableName,
//		timeoutTableName: timeoutTableName,
//		writer:           writer,
//		reader:           reader,
//	}
//
//	e.recordCols = " `id`, `workflow_name`, `foreign_id`, `run_id`, `status`, `object`, `is_start`, `is_end`, `created_at` "
//	e.recordSelectPrefix = " select " + e.recordCols + " from " + e.recordTableName + " where "
//
//	e.timeoutCols = " `id`, `workflow_name`, `foreign_id`, `run_id`, `status`, `completed`, `expire_at`, `created_at` "
//	e.timeoutSelectPrefix = " select " + e.timeoutCols + " from " + e.timeoutTableName + " where "
//
//	return e
//}
//
//var _ workflow.Store = (*SQLStore)(nil)
//
//func (s *SQLStore) Batch(ctx context.Context, workflowName string, status string, fromID int64, size int) ([]*workflow.Record, error) {
//	return s.listWhere(ctx, s.reader, "workflow_name=? and status=? and id > ? order by id asc limit ?", workflowName, status, fromID, size)
//}
//
//func (s *SQLStore) LookupLatest(ctx context.Context, key workflow.Key) (*workflow.Record, error) {
//	ls, err := s.listWhere(ctx, s.reader, "workflow_name=? and foreign_id=? and run_id=? order by id desc limit 1", key.WorkflowName, key.ForeignID, key.RunID)
//	if err != nil {
//		return nil, err
//	}
//
//	if len(ls) < 1 {
//		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
//	}
//
//	return ls[0], nil
//}
//
//func (s *SQLStore) Store(ctx context.Context, key workflow.Key, status string, object []byte, isStart, isEnd bool) error {
//	_, err := s.create(ctx, key.WorkflowName, key.ForeignID, key.RunID, status, object, isStart, isEnd)
//	return err
//}
//
//func (s *SQLStore) Find(ctx context.Context, key workflow.Key, status string) (*workflow.Record, error) {
//	return s.lookupWhere(ctx, s.reader, "workflow_name=? and foreign_id=? and run_id=? and status=?", key.WorkflowName, key.ForeignID, key.RunID, status)
//}
//
//func (s *SQLStore) LastRunID(ctx context.Context, workflowName string, foreignID string) (string, error) {
//	r, err := s.lookupWhere(ctx, s.reader, "workflow_name=? and foreign_id=? order by id desc limit 1", workflowName, foreignID)
//	if errors.Is(err, workflow.ErrRecordNotFound) {
//		return "", errors.Wrap(workflow.ErrRunIDNotFound, "")
//	} else if err != nil {
//		return "", err
//	}
//
//	return r.RunID, nil
//}
//
//func (s *SQLStore) LastRecordForWorkflow(ctx context.Context, workflowName string) (*workflow.Record, error) {
//	r, err := s.lookupWhere(ctx, s.reader, "workflow_name=? order by id desc limit 1", workflowName)
//	if err != nil {
//		return nil, err
//	}
//
//	return r, nil
//}
//
//func (s *SQLStore) WorkflowBatch(ctx context.Context, workflowName string, fromID int64, size int) ([]*workflow.Record, error) {
//	return s.listWhere(ctx, s.reader, "workflow_name=? and id > ? order by id asc limit ?", workflowName, fromID, size)
//}
//
//func (s *SQLStore) CreateTimeout(ctx context.Context, key workflow.Key, status string, expireAt time.Time) error {
//	_, err := s.writer.ExecContext(ctx, "insert into "+s.timeoutTableName+" set "+
//		" workflow_name=?, foreign_id=?, run_id=?, status=?, completed=?, expire_at=?, created_at=now() ",
//		key.WorkflowName,
//		key.ForeignID,
//		key.RunID,
//		status,
//		false,
//		expireAt,
//	)
//	if err != nil {
//		return errors.Wrap(err, "failed to create timeout", j.MKV{
//			"workflowName": key.WorkflowName,
//			"foreignID":    key.ForeignID,
//			"runID":        key.RunID,
//			"status":       status,
//			"expireAt":     expireAt,
//		})
//	}
//
//	return nil
//}
//
//func (s *SQLStore) CompleteTimeout(ctx context.Context, id int64) error {
//	_, err := s.writer.ExecContext(ctx, "update "+s.timeoutTableName+" set completed=true where id=?", id)
//	if err != nil {
//		return errors.Wrap(err, "failed to complete timeout", j.MKV{
//			"id": id,
//		})
//	}
//
//	return nil
//}
//
//func (s *SQLStore) CancelTimeout(ctx context.Context, id int64) error {
//	_, err := s.writer.ExecContext(ctx, "delete from "+s.timeoutTableName+" where id=?", id)
//	if err != nil {
//		return errors.Wrap(err, "failed to cancel / delete timeout", j.MKV{
//			"id": id,
//		})
//	}
//
//	return nil
//}
//
//func (s *SQLStore) ListValidTimeouts(ctx context.Context, workflowName string, status string, now time.Time) ([]workflow.Timeout, error) {
//	rows, err := s.reader.QueryContext(ctx, s.timeoutSelectPrefix+" workflow_name=? and status=? and expire_at<? and completed=false", workflowName, status, now)
//	if err != nil {
//		return nil, errors.Wrap(err, "list valid timeouts")
//	}
//	defer rows.Close()
//
//	var res []workflow.Timeout
//	for rows.Next() {
//		r, err := timeoutScan(rows)
//		if err != nil {
//			return nil, err
//		}
//		res = append(res, *r)
//	}
//
//	if rows.Err() != nil {
//		return nil, errors.Wrap(rows.Err(), "rows")
//	}
//
//	return res, nil
//}
