package workflow

import "github.com/luno/jettison/errors"

var (
	ErrStreamingClosed    = errors.New("streaming closed")
	ErrCursorNotFound     = errors.New("cursor not found")
	ErrRecordNotFound     = errors.New("record not found")
	ErrRunIDNotFound      = errors.New("run ID not found")
	ErrWorkflowInProgress = errors.New("current workflow still in progress - retry once complete")
)
