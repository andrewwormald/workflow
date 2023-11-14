package testing

import (
	"context"
	"fmt"
	clock_testing "k8s.io/utils/clock/testing"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/connectors/recordstores/memrecordstore"
	"github.com/andrewwormald/workflow/connectors/roleschedulers/memrolescheduler"
	"github.com/andrewwormald/workflow/connectors/timeoutstores/memtimeoutstore"
)

type SyncStatus string

const (
	SyncStatusStarted           SyncStatus = "Started"
	SyncStatusEmailSet          SyncStatus = "Email set"
	SyncStatusRegulationTimeout SyncStatus = "Regulatory cool down period"
	SyncStatusCompleted         SyncStatus = "Completed"
)

type User struct {
	UID         string
	Email       string
	CountryCode string
}

func TestStreamer(t *testing.T, constructor workflow.EventStreamerConstructor) {
	b := workflow.NewBuilder[User, SyncStatus]("sync users")
	b.AddStep(SyncStatusStarted, setEmail(), SyncStatusEmailSet)
	b.AddTimeout(SyncStatusEmailSet, CoolDownTimerFunc(), CoolDownTimeout(), SyncStatusRegulationTimeout)
	b.AddStep(SyncStatusRegulationTimeout, generateUserID(), SyncStatusCompleted)

	now := time.Date(2023, time.April, 9, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)

	wf := b.Build(
		constructor,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	ctx := context.TODO()
	wf.Run(ctx)

	foreignID := "1"
	u := User{
		CountryCode: "GB",
	}
	runId, err := wf.Trigger(ctx, foreignID, SyncStatusStarted, workflow.WithInitialValue[User, SyncStatus](&u))
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, SyncStatusEmailSet, foreignID, runId)

	clock.Step(time.Hour)

	record, err := wf.Await(ctx, foreignID, runId, SyncStatusCompleted)
	jtest.RequireNil(t, err)

	require.Equal(t, "andrew@workflow.com", record.Object.Email)
	require.Equal(t, string(SyncStatusCompleted), string(record.Status))
	require.NotEmpty(t, record.Object.UID)
}

func setEmail() func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
	return func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.Email = "andrew@workflow.com"
		fmt.Println("Set email")
		return true, nil
	}
}

func CoolDownTimerFunc() func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, time.Time, error) {
	return func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, time.Time, error) {
		fmt.Println("Cool down decider executed")

		// Place a 1-hour cool down period for Great Britain users
		if r.Object.CountryCode == "GB" {
			return true, now.Add(time.Hour), nil
		}

		// Don't provide a timeout for users outside of GB
		return false, time.Time{}, nil
	}
}

func CoolDownTimeout() func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, error) {
	return func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, error) {
		isAndrew := r.Object.Email == "andrew@workflow.com"
		fmt.Println("Cool down timeout expired")
		return isAndrew, nil
	}
}

func generateUserID() func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
	return func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.UID = uuid.New().String()
		fmt.Println("Generated user ID")
		return true, nil
	}
}
