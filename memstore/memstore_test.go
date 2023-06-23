package memstore_test

import (
	"context"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memstore"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCancelTimeout(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	err := store.CreateTimeout(ctx, workflow.Key{
		WorkflowName: "example",
		ForeignID:    "andrew",
		RunID:        "1",
	}, "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.CreateTimeout(ctx, workflow.Key{
		WorkflowName: "example",
		ForeignID:    "andrew",
		RunID:        "2",
	}, "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	err = store.CreateTimeout(ctx, workflow.Key{
		WorkflowName: "example",
		ForeignID:    "andrew",
		RunID:        "3",
	}, "Started", time.Now().Add(-time.Hour))
	jtest.RequireNil(t, err)

	timeout, err := store.ListValidTimeouts(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 3, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, int64(1), timeout[0].ID)
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Millisecond)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Millisecond)

	// Timeout 2's expectations
	require.Equal(t, int64(2), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "2", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Millisecond)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Millisecond)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[2].ID)
	require.Equal(t, "example", timeout[2].WorkflowName)
	require.Equal(t, "andrew", timeout[2].ForeignID)
	require.Equal(t, "3", timeout[2].RunID)
	require.False(t, timeout[2].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[2].ExpireAt, time.Millisecond)
	require.WithinDuration(t, time.Now(), timeout[2].CreatedAt, time.Millisecond)

	// Only cancel timeout 2 to test the in-mem implementation of slicing the timeouts and appending them together again
	err = store.CancelTimeout(ctx, 2)
	jtest.RequireNil(t, err)

	timeout, err = store.ListValidTimeouts(ctx, "example", "Started", time.Now())
	jtest.RequireNil(t, err)

	require.Equal(t, 2, len(timeout))

	// Timeout 1's expectations
	require.Equal(t, int64(1), timeout[0].ID)
	require.Equal(t, "example", timeout[0].WorkflowName)
	require.Equal(t, "andrew", timeout[0].ForeignID)
	require.Equal(t, "1", timeout[0].RunID)
	require.False(t, timeout[0].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Millisecond)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Millisecond)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "3", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Millisecond)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Millisecond)
}
