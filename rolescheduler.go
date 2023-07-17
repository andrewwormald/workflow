package workflow

import "context"

type RoleScheduler interface {
	AwaitRoleContext(ctx context.Context, role string) (context.Context, context.CancelFunc, error)
}
