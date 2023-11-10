package kafka_test

import (
	"context"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/connectors/eventstreaming/kafka"
	"github.com/andrewwormald/workflow/connectors/recordstores/memrecordstore"
	"github.com/andrewwormald/workflow/connectors/roleschedulers/memrolescheduler"
	"github.com/andrewwormald/workflow/connectors/timeoutstores/memtimeoutstore"
	"github.com/google/uuid"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
)

type SyncStatus string

const (
	SyncStatusStarted   SyncStatus = "Started"
	SyncStatusEmailSet  SyncStatus = "Email set"
	SyncStatusCompleted SyncStatus = "Completed"
)

type User struct {
	UID   string
	Email string
}

func TestStreamer(t *testing.T) {
	b := workflow.NewBuilder[User, SyncStatus]("sync users")

	b.AddStep(SyncStatusStarted, func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.Email = "andrew@workflow.com"
		return true, nil
	}, SyncStatusEmailSet)

	b.AddStep(SyncStatusEmailSet, func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.UID = uuid.New().String()
		return true, nil
	}, SyncStatusCompleted)

	wf := b.Build(
		kafka.New(),
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
		workflow.WithDebugMode(),
	)

	ctx := context.TODO()
	wf.Run(ctx)

	foreignID := "1"
	runId, err := wf.Trigger(ctx, foreignID, SyncStatusStarted)
	jtest.RequireNil(t, err)

	record, err := wf.Await(ctx, foreignID, runId, SyncStatusCompleted)
	jtest.RequireNil(t, err)

	require.Equal(t, "andrew@workflow.com", record.Object.Email)
	require.Equal(t, string(SyncStatusCompleted), string(record.Status))
	require.NotEmpty(t, record.Object.UID)
	t.Log(record)
}
