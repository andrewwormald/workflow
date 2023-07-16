package memrolescheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/andrewwormald/workflow/memrolescheduler"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestAwaitRoleContext(t *testing.T) {
	t.Run("Ensure role is locked and successive calls are blocked", func(t *testing.T) {
		rs := memrolescheduler.New()
		ctx := context.Background()
		ctxWithValue := context.WithValue(ctx, "parent", "context")

		ctx2, cancel, err := rs.AwaitRoleContext(ctxWithValue, "leader")
		jtest.RequireNil(t, err)

		t.Cleanup(cancel)

		// Ensure that the passed in context is a parent of the returned context
		require.Equal(t, "context", ctx2.Value("parent"))

		roleReleased := make(chan bool, 1)
		go func(done chan bool) {
			_, _, err := rs.AwaitRoleContext(ctxWithValue, "leader")
			jtest.RequireNil(t, err)

			roleReleased <- true

			// Ensure that the passed in context is a parent of the returned context
			require.Equal(t, "context", ctx2.Value("parent"))
		}(roleReleased)

		timeout := time.NewTicker(1 * time.Second).C
		select {
		case <-timeout:
			// Pass -  timeout expected to return first as the role has not been released
			return
		case <-roleReleased:
			t.Fail()
			return
		}
	})

	t.Run("Ensure role is released on context cancellation", func(t *testing.T) {
		rs := memrolescheduler.New()
		ctx := context.Background()

		_, cancel, err := rs.AwaitRoleContext(ctx, "leader")
		jtest.RequireNil(t, err)

		t.Cleanup(cancel)

		ctx2, cancel2 := context.WithCancel(context.Background())
		t.Cleanup(cancel2)

		roleReleased := make(chan bool, 1)
		go func(ctx context.Context, done chan bool) {
			_, _, err := rs.AwaitRoleContext(ctx2, "leader")
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
