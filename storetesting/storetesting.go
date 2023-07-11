package storetesting

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T, factory func() workflow.Store) {
	tests := []func(t *testing.T, store workflow.Store){
		testStore_LookupLatest_Find,
		testBatch,
		testCancelTimeout,
		testLastRunID,
		testLastRunIDErrRunIDNotFound,
		testLastRecordForWorkflow,
		testWorkflowBatch,
		testCompleteTimeout,
	}

	for _, test := range tests {
		storeForTesting := factory()
		test(t, storeForTesting)
	}
}

func testStore_LookupLatest_Find(t *testing.T, store workflow.Store) {
	ctx := context.Background()
	workflowName := "my_workflow"
	foreignID := "Andrew Wormald"
	runID := "LSDKLJFN-SKDFJB-WERLTBE"
	status := "Completed"

	type example struct {
		name string
	}

	e := example{name: foreignID}
	b, err := json.Marshal(e)
	jtest.RequireNil(t, err)

	key := workflow.MakeKey(workflowName, foreignID, runID)
	err = store.Store(ctx, key, status, b, true, false)
	jtest.RequireNil(t, err)

	expected := workflow.Record{
		ID:           1,
		WorkflowName: workflowName,
		ForeignID:    foreignID,
		Status:       status,
		Object:       b,
		CreatedAt:    time.Now(),
		IsStart:      true,
		IsEnd:        true,
	}

	latest, err := store.LookupLatest(ctx, key)
	jtest.RequireNil(t, err)
	recordIsEqual(t, expected, *latest)

	found, err := store.Find(ctx, key, status)
	jtest.RequireNil(t, err)
	recordIsEqual(t, expected, *found)
}

func testBatch(t *testing.T, store workflow.Store) {
	ctx := context.Background()

	type example struct {
		name string
	}

	e := example{name: "Andrew Wormald"}
	b, err := json.Marshal(e)
	jtest.RequireNil(t, err)

	records := []workflow.Record{
		{
			WorkflowName: "my_workflow",
			ForeignID:    "2903847290384",
			RunID:        "LSDKLJFN-SKDFJB-WERLTBE",
			Status:       "Started",
			IsStart:      true,
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "2903847290384",
			RunID:        "LSDKLJFN-SKDFJB-WERLTBE",
			Status:       "Completed",
			IsEnd:        true,
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			RunID:        "SDFSDF-WERRTERT-CVBNCVB",
			Status:       "Started",
			IsStart:      true,
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			RunID:        "SDFSDF-WERRTERT-CVBNCVB",
			Status:       "Completed",
			IsEnd:        true,
		},
		{
			WorkflowName: "another_workflow",
			ForeignID:    "9023874908237",
			RunID:        "KJBKJB-QWEQWOPI-PDQSDHF",
			Status:       "Started",
			IsStart:      true,
		},
		{
			WorkflowName: "another_workflow",
			ForeignID:    "9023874908237",
			RunID:        "KJBKJB-QWEQWOPI-PDQSDHF",
			Status:       "Completed",
			IsEnd:        true,
		},
	}

	now := time.Now()
	for _, record := range records {
		key := workflow.MakeKey(record.WorkflowName, record.ForeignID, record.RunID)
		err = store.Store(ctx, key, record.Status, b, record.IsStart, record.IsEnd)
		jtest.RequireNil(t, err)
	}

	ls, err := store.Batch(ctx, "my_workflow", "Started", 0, 5)
	jtest.RequireNil(t, err)

	expected := []workflow.Record{
		{
			ID:           1,
			WorkflowName: "my_workflow",
			ForeignID:    "2903847290384",
			RunID:        "LSDKLJFN-SKDFJB-WERLTBE",
			Status:       "Started",
			Object:       b,
			CreatedAt:    now,
			IsStart:      true,
		},
		{
			ID:           3,
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			RunID:        "SDFSDF-WERRTERT-CVBNCVB",
			Status:       "Started",
			Object:       b,
			CreatedAt:    now,
			IsStart:      true,
		},
	}

	for i, actual := range ls {
		recordIsEqual(t, expected[i], *actual)
	}
}

func recordIsEqual(t *testing.T, a, b workflow.Record) {
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, a.WorkflowName, b.WorkflowName)
	require.Equal(t, a.ForeignID, b.ForeignID)
	require.Equal(t, a.Status, b.Status)
	require.Equal(t, a.Object, b.Object)
	require.WithinDuration(t, a.CreatedAt, b.CreatedAt, time.Second*10)
}

func testCancelTimeout(t *testing.T, store workflow.Store) {
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
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 2's expectations
	require.Equal(t, int64(2), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "2", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[2].ID)
	require.Equal(t, "example", timeout[2].WorkflowName)
	require.Equal(t, "andrew", timeout[2].ForeignID)
	require.Equal(t, "3", timeout[2].RunID)
	require.False(t, timeout[2].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[2].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[2].CreatedAt, time.Second)

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
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "3", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)
}

func testLastRunID(t *testing.T, store workflow.Store) {
	ctx := context.Background()
	workflowName := "example_workflow_name"
	foreignID := "example_foreign_id"
	runID := "LSDKLJFN-SKDFJB-WERLTBE"
	expectedRunID := "SDFSDF-WERRTERT-CVBNCVB"

	key := workflow.MakeKey(workflowName, foreignID, runID)
	err := store.Store(ctx, key, "Completed", []byte("{example: true}"), true, false)
	jtest.RequireNil(t, err)

	newKey := workflow.MakeKey(workflowName, foreignID, expectedRunID)
	err = store.Store(ctx, newKey, "Completed", []byte("{example: true}"), true, false)
	jtest.RequireNil(t, err)

	actualRunID, err := store.LastRunID(ctx, "example_workflow_name", "example_foreign_id")
	jtest.RequireNil(t, err)

	require.Equal(t, expectedRunID, actualRunID)
}

func testLastRunIDErrRunIDNotFound(t *testing.T, store workflow.Store) {
	ctx := context.Background()
	expectedRunID := ""

	actualRunID, err := store.LastRunID(ctx, "example_workflow_name", "example_foreign_id")
	jtest.Require(t, workflow.ErrRunIDNotFound, err)

	require.Equal(t, expectedRunID, actualRunID)
}

func testLastRecordForWorkflow(t *testing.T, store workflow.Store) {
	ctx := context.Background()

	key := workflow.MakeKey("first_workflow", "example_foreign_id", "LSDKLJFN-SKDFJB-WERLTBE")
	err := store.Store(ctx, key, "Started", []byte("{example: true}"), true, false)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, key, "Completed", []byte("{example: true}"), false, true)
	jtest.RequireNil(t, err)

	newKey := workflow.MakeKey("second_workflow", "example_foreign_id", "SDFSDF-WERRTERT-CVBNCVB")
	err = store.Store(ctx, newKey, "Completed", []byte("{example: true}"), true, false)
	jtest.RequireNil(t, err)

	actualRecord, err := store.LastRecordForWorkflow(ctx, "first_workflow")
	jtest.RequireNil(t, err)

	expectedRecord := workflow.Record{
		ID:           2,
		RunID:        "LSDKLJFN-SKDFJB-WERLTBE",
		WorkflowName: "first_workflow",
		ForeignID:    "example_foreign_id",
		Status:       "Completed",
		IsStart:      false,
		IsEnd:        true,
		Object:       []byte("{example: true}"),
		CreatedAt:    time.Now(),
	}

	recordIsEqual(t, expectedRecord, *actualRecord)
}

func testWorkflowBatch(t *testing.T, store workflow.Store) {
	ctx := context.Background()
	workflowName := "example_workflow_name"
	foreignID := "example_foreign_id"
	runID := "LSDKLJFN-SKDFJB-WERLTBE"

	key := workflow.MakeKey(workflowName, foreignID, runID)
	for i := 0; i < 5; i++ {
		status := "Started"
		isStart := true
		if i%2 == 0 {
			status = "Completed"
			isStart = false
		}

		err := store.Store(ctx, key, status, []byte("{example: true}"), isStart, !isStart)
		jtest.RequireNil(t, err)
	}

	batch, err := store.WorkflowBatch(ctx, workflowName, 1, 3)
	jtest.RequireNil(t, err)

	expected := []workflow.Record{
		{
			ID:           2,
			WorkflowName: workflowName,
			ForeignID:    foreignID,
			RunID:        runID,
			Status:       "Started",
			Object:       []byte("{example: true}"),
			CreatedAt:    time.Now(),
			IsStart:      true,
		},
		{
			ID:           3,
			WorkflowName: workflowName,
			ForeignID:    foreignID,
			RunID:        runID,
			Status:       "Completed",
			Object:       []byte("{example: true}"),
			IsEnd:        true,
			CreatedAt:    time.Now(),
		},
		{
			ID:           4,
			WorkflowName: workflowName,
			ForeignID:    foreignID,
			RunID:        runID,
			Status:       "Started",
			Object:       []byte("{example: true}"),
			CreatedAt:    time.Now(),
			IsStart:      true,
		},
	}

	for i, expectedRecord := range expected {
		actualRecord := batch[i]
		recordIsEqual(t, expectedRecord, *actualRecord)
	}
}

func testCompleteTimeout(t *testing.T, store workflow.Store) {
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
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 2's expectations
	require.Equal(t, int64(2), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "2", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[2].ID)
	require.Equal(t, "example", timeout[2].WorkflowName)
	require.Equal(t, "andrew", timeout[2].ForeignID)
	require.Equal(t, "3", timeout[2].RunID)
	require.False(t, timeout[2].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[2].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[2].CreatedAt, time.Second)

	err = store.CompleteTimeout(ctx, 2)
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
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[0].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[0].CreatedAt, time.Second)

	// Timeout 3's expectations
	require.Equal(t, int64(3), timeout[1].ID)
	require.Equal(t, "example", timeout[1].WorkflowName)
	require.Equal(t, "andrew", timeout[1].ForeignID)
	require.Equal(t, "3", timeout[1].RunID)
	require.False(t, timeout[1].Completed)
	require.WithinDuration(t, time.Now().Add(-time.Hour), timeout[1].ExpireAt, time.Second)
	require.WithinDuration(t, time.Now(), timeout[1].CreatedAt, time.Second)
}
