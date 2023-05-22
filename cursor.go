package workflow

import (
	"context"
	"fmt"

	"github.com/luno/jettison/errors"
)

var ErrCursorNotFound = errors.New("cursor not found")

type Cursor interface {
	Get(ctx context.Context, name string) (string, error)
	Set(ctx context.Context, name, value string) error
}

func cursorName(workflowName string, status string) string {
	return fmt.Sprintf("%v-%v", workflowName, status)
}
