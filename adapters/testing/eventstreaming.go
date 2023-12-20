package testing

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
)

type SyncStatus int

const (
	SyncStatusUnknown           SyncStatus = 0
	SyncStatusStarted           SyncStatus = 1
	SyncStatusEmailSet          SyncStatus = 2
	SyncStatusRegulationTimeout SyncStatus = 3
	SyncStatusCompleted         SyncStatus = 4
)

func (s SyncStatus) String() string {
	switch s {
	case SyncStatusStarted:
		return "Started"
	case SyncStatusEmailSet:
		return "Email set"
	case SyncStatusRegulationTimeout:
		return "Regulatory cool down period"
	case SyncStatusCompleted:
		return "Completed"
	default:
		return "Unknown"
	}
}

type User struct {
	UID         string
	Email       string
	CountryCode string
}

func TestStreamer(t *testing.T, constructor workflow.EventStreamer) {
	b := workflow.NewBuilder[User, SyncStatus]("sync user 2")
	b.AddStep(
		SyncStatusStarted,
		setEmail(),
		SyncStatusEmailSet,
		workflow.WithStepPollingFrequency(time.Millisecond*200),
		workflow.WithParallelCount(5),
	)
	b.AddTimeout(
		SyncStatusEmailSet,
		coolDownTimerFunc(),
		coolDownTimeout(),
		SyncStatusRegulationTimeout,
		workflow.WithTimeoutPollingFrequency(time.Millisecond*200),
	)
	b.AddStep(
		SyncStatusRegulationTimeout,
		generateUserID(),
		SyncStatusCompleted,
		workflow.WithStepPollingFrequency(time.Millisecond*200),
		workflow.WithParallelCount(5),
	)

	now := time.Date(2023, time.April, 9, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)

	wf := b.Build(
		constructor,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	foreignID := "1"
	u := User{
		CountryCode: "GB",
	}
	runId, err := wf.Trigger(ctx, foreignID, SyncStatusStarted, workflow.WithInitialValue[User, SyncStatus](&u))
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, foreignID, runId, SyncStatusEmailSet)

	clock.Step(time.Hour)

	record, err := wf.Await(ctx, foreignID, runId, SyncStatusCompleted)
	jtest.RequireNil(t, err)

	require.Equal(t, "andrew@workflow.com", record.Object.Email)
	require.Equal(t, SyncStatusCompleted.String(), record.Status.String())
	require.NotEmpty(t, record.Object.UID)
}

func setEmail() func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
	return func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.Email = "andrew@workflow.com"
		return true, nil
	}
}

func coolDownTimerFunc() func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (time.Time, error) {
	return func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (time.Time, error) {
		// Place a 1-hour cool down period for Great Britain users
		if r.Object.CountryCode == "GB" {
			return now.Add(time.Hour), nil
		}

		// Don't provide a timeout for users outside of GB
		return time.Time{}, nil
	}
}

func coolDownTimeout() func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, error) {
	return func(ctx context.Context, r *workflow.Record[User, SyncStatus], now time.Time) (bool, error) {
		isAndrew := r.Object.Email == "andrew@workflow.com"
		return isAndrew, nil
	}
}

func generateUserID() func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
	return func(ctx context.Context, t *workflow.Record[User, SyncStatus]) (bool, error) {
		t.Object.UID = uuid.New().String()
		return true, nil
	}
}
