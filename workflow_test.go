package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memcursor"
	"github.com/andrewwormald/workflow/memrolescheduler"
	"github.com/andrewwormald/workflow/memstore"
)

type MyType struct {
	UserID      int64
	Profile     string
	Email       string
	Cellphone   string
	OTP         int
	OTPVerified bool
}

func (m MyType) ForeignID() string {
	return strconv.FormatInt(m.UserID, 10)
}

const (
	StatusInitiated                = "Initiated"
	StatusProfileCreated           = "Profile Created"
	StatusEmailConfirmationSent    = "Email Confirmation Sent"
	StatusEmailVerified            = "Email Verified"
	StatusCellphoneNumberSubmitted = "Cellphone Number Submitted"
	StatusOTPSent                  = "OTP Sent"
	StatusOTPVerified              = "OTP Verified"
	StatusCompleted                = "Completed"
)

type ExternalEmailVerified struct {
	IsVerified bool
}

type ExternalCellPhoneSubmitted struct {
	DialingCode string
	Number      string
}

type ExternalOTP struct {
	OTPCode int
}

func TestWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	store := memstore.New()

	b := workflow.NewBuilder[MyType]("user sign up")
	b.AddStep(StatusInitiated, createProfile, StatusProfileCreated)
	b.AddStep(StatusProfileCreated, sendEmailConfirmation, StatusEmailConfirmationSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusEmailConfirmationSent, emailVerifiedCallback, StatusEmailVerified)
	b.AddCallback(StatusEmailVerified, cellphoneNumberCallback, StatusCellphoneNumberSubmitted)
	b.AddStep(StatusCellphoneNumberSubmitted, sendOTP, StatusOTPSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusOTPSent, otpCallback, StatusOTPVerified)
	b.AddTimeout(StatusOTPVerified, workflow.DurationTimerFunc(time.Hour), waitForAccountCoolDown, StatusCompleted)

	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(
		store,
		memcursor.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	wf.Run(ctx)

	fid := strconv.FormatInt(expectedUserID, 10)

	mt := MyType{
		UserID: expectedUserID,
	}

	runID, err := wf.Trigger(ctx, fid, StatusInitiated, workflow.WithInitialValue(&mt))
	jtest.RequireNil(t, err)

	// Once in the correct state, trigger third party callbacks
	workflow.TriggerCallbackOn(t, wf, fid, StatusEmailConfirmationSent, ExternalEmailVerified{
		IsVerified: true,
	})

	workflow.TriggerCallbackOn(t, wf, fid, StatusEmailVerified, ExternalCellPhoneSubmitted{
		DialingCode: "+44",
		Number:      "7467623292",
	})

	workflow.TriggerCallbackOn(t, wf, fid, StatusOTPSent, ExternalOTP{
		OTPCode: expectedOTP,
	})

	workflow.AwaitTimeoutInsert(t, wf, StatusOTPVerified)

	// Advance time forward by one hour to trigger the timeout
	clock.Step(time.Hour)

	_, err = wf.Await(ctx, fid, StatusCompleted)
	jtest.RequireNil(t, err)

	key := workflow.MakeKey("user sign up", fid, runID)
	r, err := store.LookupLatest(ctx, key)
	jtest.RequireNil(t, err)
	require.Equal(t, expectedFinalStatus, r.Status)

	var actual MyType
	err = workflow.Unmarshal(r.Object, &actual)
	jtest.RequireNil(t, err)

	require.Equal(t, expectedUserID, actual.UserID)
	require.Equal(t, strconv.FormatInt(expectedUserID, 10), actual.ForeignID())
	require.Equal(t, expectedProfile, actual.Profile)
	require.Equal(t, expectedEmail, actual.Email)
	require.Equal(t, expectedCellphone, actual.Cellphone)
	require.Equal(t, expectedOTP, actual.OTP)
	require.Equal(t, expectedOTPVerified, actual.OTPVerified)
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	b := workflow.NewBuilder[MyType]("user sign up")

	b.AddStep(StatusInitiated, func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, StatusProfileCreated, workflow.WithStepPollingFrequency(100*time.Millisecond))

	b.AddTimeout(StatusProfileCreated, workflow.DurationTimerFunc(time.Hour), func(ctx context.Context, key workflow.Key, t *MyType, now time.Time) (bool, error) {
		return true, nil
	}, StatusCompleted, workflow.WithTimeoutPollingFrequency(100*time.Millisecond))

	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(
		memstore.New(memstore.WithClock(clock)),
		memcursor.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)
	wf.Run(ctx)

	start := time.Now()

	_, err := wf.Trigger(ctx, "example", StatusInitiated)
	jtest.RequireNil(t, err)

	_, err = wf.Await(ctx, "example", StatusProfileCreated, workflow.WithPollingFrequency(300*time.Millisecond))
	jtest.RequireNil(t, err)

	// Advance time forward by one hour to trigger the timeout
	clock.Step(time.Hour)

	_, err = wf.Await(ctx, "example", StatusCompleted)
	jtest.RequireNil(t, err)

	end := time.Now()

	fmt.Println(end.Sub(start))
	require.True(t, end.Sub(start) < 1*time.Second)
}

func TestPollingFrequency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	b := workflow.NewBuilder[MyType]("user sign up")

	b.AddStep(StatusInitiated, func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, StatusProfileCreated, workflow.WithStepPollingFrequency(100*time.Millisecond))

	b.AddStep(StatusProfileCreated, func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, StatusCompleted, workflow.WithStepPollingFrequency(100*time.Millisecond))

	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(
		memstore.New(memstore.WithClock(clock)),
		memcursor.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)
	wf.Run(ctx)

	start := time.Now()

	_, err := wf.Trigger(ctx, "example", StatusInitiated)
	jtest.RequireNil(t, err)

	_, err = wf.Await(ctx, "example", StatusCompleted, workflow.WithPollingFrequency(time.Nanosecond))
	jtest.RequireNil(t, err)

	end := time.Now()

	// Each poll is 100 milliseconds. There are 2 steps in the workflow. Total duration should be 200 milliseconds.
	require.True(t, end.Sub(start).Truncate(time.Millisecond) == 200*time.Millisecond)
}

var (
	expectedUserID      int64 = 984892374983743
	expectedFinalStatus       = StatusCompleted
	expectedProfile           = "Andrew Wormald"
	expectedEmail             = "andreww@luno.com"
	expectedCellphone         = "+44 7467623292"
	expectedOTP               = 345345
	expectedOTPVerified       = true
)

func createProfile(ctx context.Context, key workflow.Key, mt *MyType) (bool, error) {
	mt.Profile = "Andrew Wormald"
	fmt.Println("creating profile", *mt)
	return true, nil
}

func sendEmailConfirmation(ctx context.Context, key workflow.Key, mt *MyType) (bool, error) {
	fmt.Println("sending email confirmation", *mt)
	return true, nil
}

func emailVerifiedCallback(ctx context.Context, key workflow.Key, mt *MyType, r io.Reader) (bool, error) {
	fmt.Println("email verification callback", *mt)

	b, err := io.ReadAll(r)
	if err != nil {
		return false, err
	}

	var ev ExternalEmailVerified
	err = json.Unmarshal(b, &ev)
	if err != nil {
		return false, err
	}

	if ev.IsVerified {
		mt.Email = "andreww@luno.com"
	}

	return true, nil
}

func cellphoneNumberCallback(ctx context.Context, key workflow.Key, mt *MyType, r io.Reader) (bool, error) {
	fmt.Println("cell phone number callback", *mt)
	b, err := io.ReadAll(r)
	if err != nil {
		return false, err
	}

	var ev ExternalCellPhoneSubmitted
	err = json.Unmarshal(b, &ev)
	if err != nil {
		return false, err
	}

	if ev.DialingCode != "" && ev.Number != "" {
		mt.Cellphone = fmt.Sprintf("%v %v", ev.DialingCode, ev.Number)
	}

	return true, nil
}

func sendOTP(ctx context.Context, key workflow.Key, mt *MyType) (bool, error) {
	fmt.Println("send otp", *mt)
	mt.OTP = expectedOTP
	return true, nil
}

func otpCallback(ctx context.Context, key workflow.Key, mt *MyType, r io.Reader) (bool, error) {
	fmt.Println("otp callback", *mt)
	b, err := io.ReadAll(r)
	if err != nil {
		return false, err
	}

	var otp ExternalOTP
	err = json.Unmarshal(b, &otp)
	if err != nil {
		return false, err
	}

	if otp.OTPCode == expectedOTP {
		mt.OTPVerified = true
	}

	return true, nil
}

func waitForAccountCoolDown(ctx context.Context, key workflow.Key, mt *MyType, now time.Time) (bool, error) {
	fmt.Println(fmt.Sprintf("completed waiting for account cool down %v at %v", *mt, now.String()))
	return true, nil
}

func TestNot(t *testing.T) {
	t.Run("Not - flip true to false", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, key workflow.Key, s *string) (bool, error) {
			return true, nil
		})

		key := workflow.MakeKey("w", "f", "r")
		s := "example"
		actual, err := fn(context.Background(), key, &s)
		jtest.RequireNil(t, err)
		require.False(t, actual)
	})

	t.Run("Not - flip false to true", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, key workflow.Key, s *string) (bool, error) {
			return false, nil
		})

		key := workflow.MakeKey("w", "f", "r")
		s := "example"
		actual, err := fn(context.Background(), key, &s)
		jtest.RequireNil(t, err)
		require.True(t, actual)
	})

	t.Run("Not - propagate error and do not flip result to true", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, key workflow.Key, s *string) (bool, error) {
			return false, errors.New("expected error")
		})

		key := workflow.MakeKey("w", "f", "r")
		s := "example"
		actual, err := fn(context.Background(), key, &s)
		jtest.Require(t, err, errors.New("expected error"))
		require.False(t, actual)
	})
}

func TestWorkflow_ScheduleTrigger(t *testing.T) {
	now := time.Date(2023, time.April, 9, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)
	store := memstore.New(memstore.WithClock(clock))
	b := workflow.NewBuilder[MyType]("sync users")

	b.AddStep("Started", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Collected users", workflow.WithStepPollingFrequency(time.Millisecond))

	b.AddStep("Collected users", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Synced users", workflow.WithStepPollingFrequency(time.Millisecond))

	wf := b.Build(
		store,
		memcursor.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	wf.Run(ctx)

	go func() {
		err := wf.ScheduleTrigger(ctx, "andrew", "Started", "@monthly")
		jtest.RequireNil(t, err)
	}()

	time.Sleep(20 * time.Millisecond)

	runID, err := store.LastRunID(ctx, "sync users", "andrew")
	// Expect there to be no entries yet
	jtest.Require(t, workflow.ErrRunIDNotFound, err)

	// Grab the time from the clock for expectation as to the time we expect the entry to have
	expectedTimestamp := time.Date(2023, time.May, 1, 0, 0, 0, 0, time.UTC)
	clock.SetTime(expectedTimestamp)

	// Allow scheduling to take place
	time.Sleep(20 * time.Millisecond)

	runID, err = store.LastRunID(ctx, "sync users", "andrew")
	jtest.RequireNil(t, err)

	key := workflow.MakeKey("sync users", "andrew", runID)
	latest, err := store.LookupLatest(ctx, key)
	jtest.RequireNil(t, err)

	var mt MyType
	serialised, err := workflow.Marshal(&mt)
	jtest.RequireNil(t, err)

	expected := workflow.Record{
		ID:           3,
		RunID:        runID,
		WorkflowName: "sync users",
		ForeignID:    "andrew",
		Status:       "Synced users",
		IsStart:      false,
		IsEnd:        true,
		Object:       serialised,
		CreatedAt:    expectedTimestamp,
	}
	require.Equal(t, expected, *latest)

	expectedTimestamp = time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	clock.SetTime(expectedTimestamp)

	// Allow scheduling to take place
	time.Sleep(20 * time.Millisecond)

	runID, err = store.LastRunID(ctx, "sync users", "andrew")
	jtest.RequireNil(t, err)

	key = workflow.MakeKey("sync users", "andrew", runID)
	latest, err = store.LookupLatest(ctx, key)
	jtest.RequireNil(t, err)

	secondExpected := workflow.Record{
		ID:           6,
		RunID:        runID,
		WorkflowName: "sync users",
		ForeignID:    "andrew",
		Status:       "Synced users",
		IsStart:      false,
		IsEnd:        true,
		Object:       serialised,
		CreatedAt:    expectedTimestamp,
	}
	require.Equal(t, secondExpected, *latest)
}

func TestWorkflow_ErrWorkflowNotRunning(t *testing.T) {
	b := workflow.NewBuilder[MyType]("sync users")

	b.AddStep("Started", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Collected users", workflow.WithStepPollingFrequency(time.Millisecond))

	b.AddStep("Collected users", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Synced users", workflow.WithStepPollingFrequency(time.Millisecond))

	wf := b.Build(
		memstore.New(),
		memcursor.New(),
		memrolescheduler.New(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	_, err := wf.Trigger(ctx, "andrew", "Started")
	jtest.Require(t, workflow.ErrWorkflowNotRunning, err)

	err = wf.ScheduleTrigger(ctx, "andrew", "Started", "@monthly")
	jtest.Require(t, workflow.ErrWorkflowNotRunning, err)
}

func TestWorkflow_TestingRequire(t *testing.T) {
	b := workflow.NewBuilder[MyType]("sync users")

	b.AddStep("Started", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		t.Email = "andrew@workflow.com"
		return true, nil
	}, "Updated email", workflow.WithStepPollingFrequency(time.Millisecond))

	b.AddStep("Updated email", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		t.Cellphone = "+44 349 8594"
		return true, nil
	}, "Updated cellphone", workflow.WithStepPollingFrequency(time.Millisecond))

	wf := b.Build(
		memstore.New(),
		memcursor.New(),
		memrolescheduler.New(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	wf.Run(ctx)

	_, err := wf.Trigger(ctx, "andrew", "Started")
	jtest.RequireNil(t, err)

	expected := MyType{
		Email: "andrew@workflow.com",
	}
	workflow.Require(t, wf, "Updated email", expected)

	expected = MyType{
		Email:     "andrew@workflow.com",
		Cellphone: "+44 349 8594",
	}
	workflow.Require(t, wf, "Updated cellphone", expected)
}

func TestTimeTimerFunc(t *testing.T) {
	type YinYang struct {
		Yin  bool
		Yang bool
	}

	b := workflow.NewBuilder[YinYang]("timer_func")

	launchDate := time.Date(1992, time.April, 9, 0, 0, 0, 0, time.UTC)
	b.AddTimeout("Pending", workflow.TimeTimerFunc(launchDate), func(ctx context.Context, key workflow.Key, t *YinYang, now time.Time) (bool, error) {
		t.Yin = true
		t.Yang = true
		return true, nil
	},
		"Launched",
	)

	now := time.Date(1991, time.December, 25, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)

	wf := b.Build(
		memstore.New(),
		memcursor.New(),
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	ctx := context.Background()
	wf.Run(ctx)

	_, err := wf.Trigger(ctx, "Andrew Wormald", "Pending")
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, "Pending")

	clock.SetTime(launchDate)

	expected := YinYang{
		Yin:  true,
		Yang: true,
	}
	workflow.Require(t, wf, "Launched", expected)
}
