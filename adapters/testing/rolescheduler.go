package testing

import (
	"context"
	"fmt"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"testing"
	"time"
)

func TestRoleScheduler(t *testing.T, factory func() workflow.RoleScheduler) {
	tests := []func(t *testing.T, store workflow.RoleScheduler){
		locking,
		cancelRelease,
	}

	for _, test := range tests {
		schedulerForTesting := factory()
		test(t, schedulerForTesting)
	}
}

func locking(t *testing.T, scheduler workflow.RoleScheduler) {
	t.Run("Ensure role is locked and successive calls are blocked", func(t *testing.T) {
		ctx := context.Background()
		ctxWithValue := context.WithValue(ctx, "parent", "context")

		fmt.Println("waiting for role")
		ctx2, cancel, err := scheduler.Await(ctxWithValue, "leader")
		jtest.RequireNil(t, err)
		fmt.Println("got role")

		t.Cleanup(func() {
			scheduler.Stop(context.Background())
			cancel()
			goleak.VerifyNone(t)
		})

		// Ensure that the passed in context is a parent of the returned context
		require.Equal(t, "context", ctx2.Value("parent"))

		roleReleased := make(chan bool, 1)
		go func(done chan bool) {
			fmt.Println("asking for role 2")
			_, cancel, err := scheduler.Await(ctxWithValue, "leader")
			jtest.RequireNil(t, err)
			t.Cleanup(cancel)
			fmt.Println("got role 2")

			roleReleased <- true
			// Ensure that the passed in context is a parent of the returned context
			require.Equal(t, "context", ctx2.Value("parent"))
		}(roleReleased)

		timeout := time.NewTicker(1 * time.Second).C
		select {
		case <-timeout:
			// Pass - timeout expected to return first as the role has not been released
			return
		case <-roleReleased:
			t.Fail()
			return
		}
	})
}

func cancelRelease(t *testing.T, scheduler workflow.RoleScheduler) {
	t.Run("Ensure role is released on context cancellation", func(t *testing.T) {
		ctx := context.Background()

		_, cancel, err := scheduler.Await(ctx, "leader")
		jtest.RequireNil(t, err)

		t.Cleanup(func() {
			scheduler.Stop(context.Background())
			cancel()
			//goleak.VerifyNone(t)
		})

		ctx2, cancel2 := context.WithCancel(context.Background())
		t.Cleanup(cancel2)

		roleReleased := make(chan bool, 1)
		go func(ctx context.Context, done chan bool) {
			_, _, err := scheduler.Await(ctx2, "leader")
			jtest.RequireNil(t, err)

			roleReleased <- true
		}(ctx2, roleReleased)

		cancel()

		timeout := time.NewTicker(3 * time.Second).C
		select {
		case <-timeout:
			t.Fail()
			return
		case <-roleReleased:
			// Pass - context passed into the first attempt at gaining the role is cancelled and should
			// result in a releasing of the role.
			return
		}
	})
}
