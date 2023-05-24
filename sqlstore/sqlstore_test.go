package sqlstore_test

import (
	"context"
	"encoding/json"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/sqlstore"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSQLStore_Store_LookupLatest_Find(t *testing.T) {
	dbc := ConnectForTesting(t)

	store := sqlstore.New("workflow_entries", dbc, dbc)

	ctx := context.Background()
	workflowName := "my_workflow"
	foreignID := "Andrew Wormald"
	status := "Completed"

	type example struct {
		name string
	}

	e := example{name: foreignID}
	b, err := json.Marshal(e)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, workflowName, foreignID, status, b)
	jtest.RequireNil(t, err)

	expected := workflow.Record{
		ID:           1,
		WorkflowName: workflowName,
		ForeignID:    foreignID,
		Status:       status,
		Object:       b,
		CreatedAt:    time.Now(),
	}

	latest, err := store.LookupLatest(ctx, workflowName, foreignID)
	jtest.RequireNil(t, err)
	recordIsEqual(t, expected, *latest)

	found, err := store.Find(ctx, workflowName, foreignID, status)
	jtest.RequireNil(t, err)
	recordIsEqual(t, expected, *found)
}

func TestSQLStore_Batch(t *testing.T) {
	dbc := ConnectForTesting(t)

	store := sqlstore.New("workflow_entries", dbc, dbc)

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
			Status:       "Started",
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "2903847290384",
			Status:       "Completed",
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			Status:       "Started",
		},
		{
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			Status:       "Completed",
		},
		{
			WorkflowName: "another_workflow",
			ForeignID:    "9023874908237",
			Status:       "Started",
		},
		{
			WorkflowName: "another_workflow",
			ForeignID:    "9023874908237",
			Status:       "Completed",
		},
	}

	now := time.Now()
	for _, record := range records {
		err = store.Store(ctx, record.WorkflowName, record.ForeignID, record.Status, b)
		jtest.RequireNil(t, err)
	}

	ls, err := store.Batch(ctx, "my_workflow", "Started", 0, 5)
	jtest.RequireNil(t, err)

	expected := []workflow.Record{
		{
			ID:           1,
			WorkflowName: "my_workflow",
			ForeignID:    "2903847290384",
			Status:       "Started",
			Object:       b,
			CreatedAt:    now,
		},
		{
			ID:           3,
			WorkflowName: "my_workflow",
			ForeignID:    "9873495873498",
			Status:       "Started",
			Object:       b,
			CreatedAt:    now,
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
