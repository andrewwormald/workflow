package workflow

import "context"

type RoleScheduleFunc func(ctx context.Context, role string) (context.Context, context.CancelFunc, error)

type RoleScheduler interface {
	AwaitRoleContext(ctx context.Context, role string) (context.Context, context.CancelFunc, error)
}
