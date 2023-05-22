package sqlcursor

import (
	"context"
	"database/sql"
	"github.com/luno/jettison/j"

	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

func (s *SQLCursor) create(ctx context.Context, name string, value string) (int64, error) {
	resp, err := s.writer.ExecContext(ctx, "insert into "+s.name+" set "+
		" name=?, value=?, created_at=now(), updated_at=now();",
		name,
		value,
	)

	if err != nil {
		return 0, errors.Wrap(err, "failed to create entry", j.MKV{
			"name":  name,
			"value": value,
		})
	}

	return resp.LastInsertId()
}

func (s *SQLCursor) update(ctx context.Context, id int64, name string, value ...any) error {
	_, err := s.writer.ExecContext(ctx, "update "+s.name+" set  name=?, value=?, updated_at=now() where id=?;",
		name,
		value,
		id,
	)

	if err != nil {
		return errors.Wrap(err, "failed to update entry", j.MKV{
			"id":    id,
			"name":  name,
			"value": value,
		})
	}

	return nil
}

func (s *SQLCursor) lookupWhere(ctx context.Context, dbc *sql.DB, where string, args ...any) (*CursorEntry, error) {
	return scan(dbc.QueryRowContext(ctx, s.selectPrefix+where, args...))
}

// listWhere queries the table with the provided where clause, then scans
// and returns all the rows.
func (s *SQLCursor) listWhere(ctx context.Context, dbc *sql.DB, where string, args ...any) ([]CursorEntry, error) {
	rows, err := dbc.QueryContext(ctx, s.selectPrefix+where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "list")
	}
	defer rows.Close()

	var res []CursorEntry
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, *r)
	}

	if rows.Err() != nil {
		return nil, errors.Wrap(rows.Err(), "rows")
	}

	return res, nil
}

func scan(row row) (*CursorEntry, error) {
	var e CursorEntry
	err := row.Scan(&e.ID, &e.Name, &e.Value, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Wrap(workflow.ErrCursorNotFound, "")
	} else if err != nil {
		return nil, errors.Wrap(err, "scan")
	}

	return &e, nil
}

// row is a common interface for *sql.Rows and *sql.Row.
type row interface {
	Scan(dest ...any) error
}
