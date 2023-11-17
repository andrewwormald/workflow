package workflow

import "context"

type Producer interface {
	Send(ctx context.Context, e *Event) error
	Close() error
}
