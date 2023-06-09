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
	// This introduces a validation element to the producing of events.
	ctx := context.Background()
	store := memstore.New()
	cursor := memcursor.New()

	b := workflow.NewBuilder[MyType]("user sign up", store, cursor)
	b.AddStep(StatusInitiated, createProfile, StatusProfileCreated)
	b.AddStep(StatusProfileCreated, sendEmailConfirmation, StatusEmailConfirmationSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusEmailConfirmationSent, emailVerifiedCallback, StatusEmailVerified)
	b.AddCallback(StatusEmailVerified, cellphoneNumberCallback, StatusCellphoneNumberSubmitted)
	b.AddStep(StatusCellphoneNumberSubmitted, sendOTP, StatusOTPSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusOTPSent, otpCallback, StatusOTPVerified)
	b.AddTimeoutWithDuration(StatusOTPVerified, waitForAccountCoolDown, time.Hour, StatusCompleted)

	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(workflow.WithClock(clock))

	wf.Run(ctx)

	fid := strconv.FormatInt(expectedUserID, 10)

	mt := MyType{
		UserID: expectedUserID,
	}

	runID, err := wf.Trigger(ctx, fid, StatusInitiated, workflow.WithInitialValue(&mt))
	jtest.RequireNil(t, err)

	// Once in the correct state, trigger third party callbacks
	workflow.TriggerCallbackOn(t, wf, fid, runID, StatusEmailConfirmationSent, ExternalEmailVerified{
		IsVerified: true,
	})

	workflow.TriggerCallbackOn(t, wf, fid, runID, StatusEmailVerified, ExternalCellPhoneSubmitted{
		DialingCode: "+44",
		Number:      "7467623292",
	})

	workflow.TriggerCallbackOn(t, wf, fid, runID, StatusOTPSent, ExternalOTP{
		OTPCode: expectedOTP,
	})

	workflow.AwaitTimeoutInsert(t, wf, StatusOTPVerified)

	// Advance time forward by one hour to trigger the timeout
	clock.Step(time.Hour)

	_, err = wf.Await(ctx, fid, runID, StatusCompleted)
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

func TestPollingFrequency(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	cursor := memcursor.New()

	b := workflow.NewBuilder[MyType]("user sign up", store, cursor)

	b.AddStep(StatusInitiated, func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, StatusProfileCreated, workflow.WithStepPollingFrequency(100*time.Millisecond))

	b.AddTimeoutWithDuration(StatusProfileCreated, func(ctx context.Context, key workflow.Key, t *MyType, now time.Time) (bool, error) {
		return true, nil
	}, time.Hour, StatusCompleted, workflow.WithTimeoutPollingFrequency(100*time.Millisecond))

	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(workflow.WithClock(clock))
	wf.Run(ctx)

	start := time.Now()

	runID, err := wf.Trigger(ctx, "example", StatusInitiated)
	jtest.RequireNil(t, err)

	_, err = wf.Await(ctx, "example", runID, StatusProfileCreated, workflow.WithPollingFrequency(300*time.Millisecond))
	jtest.RequireNil(t, err)

	// Advance time forward by one hour to trigger the timeout
	clock.Step(time.Hour)

	_, err = wf.Await(ctx, "example", runID, StatusCompleted)
	jtest.RequireNil(t, err)

	end := time.Now()

	fmt.Println(end.Sub(start))
	require.True(t, end.Sub(start) < 1*time.Second)
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
	cursor := memcursor.New()
	b := workflow.NewBuilder[MyType]("sync users", store, cursor)

	b.AddStep("Started", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Collected users", workflow.WithStepPollingFrequency(time.Millisecond))

	b.AddStep("Collected users", func(ctx context.Context, key workflow.Key, t *MyType) (bool, error) {
		return true, nil
	}, "Synced users", workflow.WithStepPollingFrequency(time.Millisecond))

	wf := b.Build(workflow.WithClock(clock))
	ctx := context.Background()
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
