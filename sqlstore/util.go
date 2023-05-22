package sqlstore

import (
	"context"
	"database/sql"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"

	"github.com/andrewwormald/workflow"
)

func (s *SQLStore) create(ctx context.Context, workflowName string, foreignID string, status string, object []byte) (int64, error) {
	resp, err := s.writer.ExecContext(ctx, "insert into "+s.name+" set "+
		" workflow_name=?, foreign_id=?, status=?, object=?, created_at=now() ",
		workflowName,
		foreignID,
		status,
		object,
	)

	if err != nil {
		return 0, errors.Wrap(err, "failed to create entry", j.MKV{
			"workflowName": workflowName,
			"foreignID":    foreignID,
			"status":       status,
			"object":       object,
		})
	}

	return resp.LastInsertId()
}

func (s *SQLStore) lookupWhere(ctx context.Context, dbc *sql.DB, where string, args ...any) (*workflow.Record, error) {
	return scan(dbc.QueryRowContext(ctx, s.selectPrefix+where, args...))
}

// listWhere queries the table with the provided where clause, then scans
// and returns all the rows.
func (s *SQLStore) listWhere(ctx context.Context, dbc *sql.DB, where string, args ...any) ([]*workflow.Record, error) {
	rows, err := dbc.QueryContext(ctx, s.selectPrefix+where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "list")
	}
	defer rows.Close()

	var res []*workflow.Record
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}

	if rows.Err() != nil {
		return nil, errors.Wrap(rows.Err(), "rows")
	}

	return res, nil
}

func scan(row row) (*workflow.Record, error) {
	var r workflow.Record
	err := row.Scan(&r.ID, &r.WorkflowName, &r.ForeignID, &r.Status, &r.Object, &r.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Wrap(workflow.ErrRecordNotFound, "")
	} else if err != nil {
		return nil, errors.Wrap(err, "scan")
	}

	return &r, nil
}

// row is a common interface for *sql.Rows and *sql.Row.
type row interface {
	Scan(dest ...any) error
}
