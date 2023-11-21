package testing

import (
	"context"
	"testing"
	"time"

	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestTimeoutStore(t *testing.T, factory func() workflow.TimeoutStore) {
	tests := []func(t *testing.T, store workflow.TimeoutStore){
		testCancelTimeout,
		testCompleteTimeout,
	}

	for _, test := range tests {
		storeForTesting := factory()
		test(t, storeForTesting)
	}
}

func testCancelTimeout(t *testing.T, store workflow.TimeoutStore) {
	ctx := context.Background()

	err := store.Create(ctx, "example", "andrew", "1", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.Create(ctx, "example", "andrew", "2", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.Create(ctx, "example", "andrew", "3", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	timeout, err := store.ListValid(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 3, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 2's expectations
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "2", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, "example", timeout[2].WorkflowName)
	require.Equal(t, "andrew", timeout[2].ForeignID)
	require.Equal(t, "3", timeout[2].RunID)
	require.False(t, timeout[2].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[2].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[2].CreatedAt, time.Second)

	err = store.Cancel(ctx, "example", "andrew", "2", "Started")
	jtest.RequireNil(t, err)

	timeout, err = store.ListValid(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 2, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "3", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)
}

func testCompleteTimeout(t *testing.T, store workflow.TimeoutStore) {
	ctx := context.Background()

	err := store.Create(ctx, "example", "andrew", "1", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.Create(ctx, "example", "andrew", "2", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.Create(ctx, "example", "andrew", "3", "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	timeout, err := store.ListValid(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 3, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 2's expectations
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "2", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, "example", timeout[2].WorkflowName)
	require.Equal(t, "andrew", timeout[2].ForeignID)
	require.Equal(t, "3", timeout[2].RunID)
	require.False(t, timeout[2].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[2].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[2].CreatedAt, time.Second)

	err = store.Complete(ctx, "example", "andrew", "2", "Started")
	jtest.RequireNil(t, err)

	timeout, err = store.ListValid(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 2, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "3", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)
}
