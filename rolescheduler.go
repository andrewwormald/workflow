package workflow

import "context"

type RoleScheduler interface {
	Await(ctx context.Context, role string) (context.Context, context.CancelFunc, error)
}
