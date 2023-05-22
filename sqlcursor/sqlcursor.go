package sqlcursor

import (
	"context"
	"database/sql"
	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

type SQLCursor struct {
	writer *sql.DB
	reader *sql.DB

	name         string
	cols         string
	selectPrefix string
}

func New(name string, writer *sql.DB, reader *sql.DB) *SQLCursor {
	e := &SQLCursor{
		name:   name,
		writer: writer,
		reader: reader,
	}

	e.cols = " `id`, `name`, `value`, `created_at`, `updated_at` "
	e.selectPrefix = " select " + e.cols + " from " + name + " where "

	return e
}

var _ workflow.Cursor = (*SQLCursor)(nil)

func (s *SQLCursor) Get(ctx context.Context, name string) (string, error) {
	ce, err := s.lookupWhere(ctx, s.reader, "name=?", name)
	if err != nil {
		return "", err
	}

	return ce.Value, nil
}

func (s *SQLCursor) Set(ctx context.Context, name, value string) error {
	exists := true
	entry, err := scan(s.writer.QueryRowContext(ctx, s.selectPrefix+"name=?", name))
	if errors.Is(err, workflow.ErrCursorNotFound) {
		exists = false
	} else if err != nil {
		return err
	}

	if !exists {
		_, err := s.create(ctx, name, value)
		return err
	} else {
		return s.update(ctx, entry.ID, name, value)
	}
}
