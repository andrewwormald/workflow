package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memstreamer"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
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

	b := workflow.NewBuilder[MyType, string]("user sign up")
	b.AddStep(StatusInitiated, createProfile, StatusProfileCreated)
	b.AddStep(StatusProfileCreated, sendEmailConfirmation, StatusEmailConfirmationSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusEmailConfirmationSent, emailVerifiedCallback, StatusEmailVerified)
	b.AddCallback(StatusEmailVerified, cellphoneNumberCallback, StatusCellphoneNumberSubmitted)
	b.AddStep(StatusCellphoneNumberSubmitted, sendOTP, StatusOTPSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusOTPSent, otpCallback, StatusOTPVerified)
	b.AddTimeout(StatusOTPVerified, workflow.DurationTimerFunc[MyType, string](time.Hour), waitForAccountCoolDown, StatusCompleted)

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
		workflow.WithClock(clock),
		workflow.WithDebugMode(),
	)

	wf.Run(ctx)

	fid := strconv.FormatInt(expectedUserID, 10)

	mt := MyType{
		UserID: expectedUserID,
	}

	runID, err := wf.Trigger(ctx, fid, StatusInitiated, workflow.WithInitialValue[MyType, string](&mt))
	jtest.RequireNil(t, err)

	// Once in the correct State, trigger third party callbacks
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

	workflow.AwaitTimeoutInsert(t, wf, fid, runID, StatusOTPVerified)

	// Advance time forward by one hour to trigger the timeout
	clock.Step(time.Hour)

	_, err = wf.Await(ctx, fid, runID, StatusCompleted)
	jtest.RequireNil(t, err)

	r, err := recordStore.Latest(ctx, "user sign up", fid)
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

	b := workflow.NewBuilder[MyType, string]("user sign up")

	b.AddStep(StatusInitiated, func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, StatusProfileCreated)

	b.AddTimeout(StatusProfileCreated, workflow.DurationTimerFunc[MyType, string](time.Hour), func(ctx context.Context, t *workflow.Record[MyType, string], now time.Time) (bool, error) {
		return true, nil
	}, StatusCompleted, workflow.WithTimeoutPollingFrequency(100*time.Millisecond))

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	clock := clock_testing.NewFakeClock(time.Now())
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	wf.Run(ctx)

	start := time.Now()

	runID, err := wf.Trigger(ctx, "example", StatusInitiated)
	jtest.RequireNil(t, err)

	_, err = wf.Await(ctx, "example", runID, StatusProfileCreated)
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, "example", runID, StatusProfileCreated)

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

func createProfile(ctx context.Context, mt *workflow.Record[MyType, string]) (bool, error) {
	mt.Object.Profile = "Andrew Wormald"
	fmt.Println("creating profile", *mt)
	return true, nil
}

func sendEmailConfirmation(ctx context.Context, mt *workflow.Record[MyType, string]) (bool, error) {
	fmt.Println("sending email confirmation", *mt)
	return true, nil
}

func emailVerifiedCallback(ctx context.Context, mt *workflow.Record[MyType, string], r io.Reader) (bool, error) {
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
		mt.Object.Email = "andreww@luno.com"
	}

	return true, nil
}

func cellphoneNumberCallback(ctx context.Context, mt *workflow.Record[MyType, string], r io.Reader) (bool, error) {
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
		mt.Object.Cellphone = fmt.Sprintf("%v %v", ev.DialingCode, ev.Number)
	}

	return true, nil
}

func sendOTP(ctx context.Context, mt *workflow.Record[MyType, string]) (bool, error) {
	fmt.Println("send otp", *mt)
	mt.Object.OTP = expectedOTP
	return true, nil
}

func otpCallback(ctx context.Context, mt *workflow.Record[MyType, string], r io.Reader) (bool, error) {
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
		mt.Object.OTPVerified = true
	}

	return true, nil
}

func waitForAccountCoolDown(ctx context.Context, mt *workflow.Record[MyType, string], now time.Time) (bool, error) {
	fmt.Println(fmt.Sprintf("completed waiting for account cool down %v at %v", *mt, now.String()))
	return true, nil
}

func TestNot(t *testing.T) {
	t.Run("Not - flip true to false", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, s *workflow.Record[string, string]) (bool, error) {
			return true, nil
		})

		s := "example"
		r := workflow.Record[string, string]{
			Object: &s,
		}
		actual, err := fn(context.Background(), &r)
		jtest.RequireNil(t, err)
		require.False(t, actual)
	})

	t.Run("Not - flip false to true", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, s *workflow.Record[string, string]) (bool, error) {
			return false, nil
		})

		s := "example"
		r := workflow.Record[string, string]{
			Object: &s,
		}
		actual, err := fn(context.Background(), &r)
		jtest.RequireNil(t, err)
		require.True(t, actual)
	})

	t.Run("Not - propagate error and do not flip result to true", func(t *testing.T) {
		fn := workflow.Not[string](func(ctx context.Context, s *workflow.Record[string, string]) (bool, error) {
			return false, errors.New("expected error")
		})

		s := "example"
		r := workflow.Record[string, string]{
			Object: &s,
		}
		actual, err := fn(context.Background(), &r)
		jtest.Require(t, err, errors.New("expected error"))
		require.False(t, actual)
	})
}

func TestWorkflow_ScheduleTrigger(t *testing.T) {
	b := workflow.NewBuilder[MyType, string]("sync users")
	b.AddStep("Started", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, "Collected users")

	b.AddStep("Collected users", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, "Synced users")

	now := time.Date(2023, time.April, 9, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)
	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
		workflow.WithClock(clock),
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	wf.Run(ctx)

	go func() {
		err := wf.ScheduleTrigger("andrew", "Started", "@monthly")
		jtest.RequireNil(t, err)
	}()

	time.Sleep(20 * time.Millisecond)

	_, err := recordStore.Latest(ctx, "sync users", "andrew")
	// Expect there to be no entries yet
	jtest.Require(t, workflow.ErrRecordNotFound, err)

	// Grab the time from the clock for expectation as to the time we expect the entry to have
	expectedTimestamp := time.Date(2023, time.May, 1, 0, 0, 0, 0, time.UTC)
	clock.SetTime(expectedTimestamp)

	// Allow scheduling to take place
	time.Sleep(20 * time.Millisecond)

	firstScheduled, err := recordStore.Latest(ctx, "sync users", "andrew")
	jtest.RequireNil(t, err)

	_, err = wf.Await(ctx, firstScheduled.ForeignID, firstScheduled.RunID, "Synced users")
	jtest.RequireNil(t, err)

	expectedTimestamp = time.Date(2023, time.June, 1, 0, 0, 0, 0, time.UTC)
	clock.SetTime(expectedTimestamp)

	// Allow scheduling to take place
	time.Sleep(20 * time.Millisecond)

	secondScheduled, err := recordStore.Latest(ctx, "sync users", "andrew")
	jtest.RequireNil(t, err)

	require.NotEqual(t, firstScheduled.RunID, secondScheduled.RunID)
}

func TestWorkflow_ScheduleTriggerShutdown(t *testing.T) {
	b := workflow.NewBuilder[MyType, string]("example")
	b.AddStep("Started", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, "End")

	wf := b.Build(
		memstreamer.New(),
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
		workflow.WithDebugMode(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	wf.Run(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		err := wf.ScheduleTrigger("andrew", "Started", "@monthly")
		jtest.RequireNil(t, err)
	}()

	wg.Wait()

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, map[string]workflow.State{
		"example-Started-andrew-scheduler-@monthly": workflow.StateRunning,
		"example-started-to-end-consumer-1-of-1":    workflow.StateRunning,
	}, wf.States())

	wf.Stop()

	require.Equal(t, map[string]workflow.State{
		"example-Started-andrew-scheduler-@monthly": workflow.StateShutdown,
		"example-started-to-end-consumer-1-of-1":    workflow.StateShutdown,
	}, wf.States())
}

func TestWorkflow_ErrWorkflowNotRunning(t *testing.T) {
	b := workflow.NewBuilder[MyType, string]("sync users")

	b.AddStep("Started", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, "Collected users")

	b.AddStep("Collected users", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		return true, nil
	}, "Synced users")

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	_, err := wf.Trigger(ctx, "andrew", "Started")
	jtest.Require(t, workflow.ErrWorkflowNotRunning, err)

	err = wf.ScheduleTrigger("andrew", "Started", "@monthly")
	jtest.Require(t, workflow.ErrWorkflowNotRunning, err)
}

func TestWorkflow_TestingRequire(t *testing.T) {
	b := workflow.NewBuilder[MyType, string]("sync users")

	b.AddStep("Started", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		t.Object.Email = "andrew@workflow.com"
		return true, nil
	}, "Updated email")

	b.AddStep("Updated email", func(ctx context.Context, t *workflow.Record[MyType, string]) (bool, error) {
		t.Object.Cellphone = "+44 349 8594"
		return true, nil
	}, "Updated cellphone")

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	wf.Run(ctx)

	foreignID := "andrew"
	runID, err := wf.Trigger(ctx, foreignID, "Started")
	jtest.RequireNil(t, err)

	expected := MyType{
		Email: "andrew@workflow.com",
	}
	workflow.Require(t, wf, foreignID, runID, "Updated email", expected)

	expected = MyType{
		Email:     "andrew@workflow.com",
		Cellphone: "+44 349 8594",
	}
	workflow.Require(t, wf, foreignID, runID, "Updated cellphone", expected)
}

func TestTimeTimerFunc(t *testing.T) {
	type YinYang struct {
		Yin  bool
		Yang bool
	}

	b := workflow.NewBuilder[YinYang, string]("timer_func")

	launchDate := time.Date(1992, time.April, 9, 0, 0, 0, 0, time.UTC)
	b.AddTimeout("Pending", workflow.TimeTimerFunc[YinYang, string](launchDate), func(ctx context.Context, t *workflow.Record[YinYang, string], now time.Time) (bool, error) {
		t.Object.Yin = true
		t.Object.Yang = true
		return true, nil
	},
		"Launched",
	)

	now := time.Date(1991, time.December, 25, 8, 30, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(now)

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	wf.Run(ctx)

	runID, err := wf.Trigger(ctx, "Andrew Wormald", "Pending")
	jtest.RequireNil(t, err)

	workflow.AwaitTimeoutInsert(t, wf, "Andrew Wormald", runID, "Pending")

	clock.SetTime(launchDate)

	expected := YinYang{
		Yin:  true,
		Yang: true,
	}
	workflow.Require(t, wf, "Andrew Wormald", runID, "Launched", expected)
}

func TestInternalState(t *testing.T) {
	b := workflow.NewBuilder[string, string]("example")
	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[string, string]) (bool, error) {
		return true, nil
	}, "Middle")

	b.AddStep("Middle", func(ctx context.Context, r *workflow.Record[string, string]) (bool, error) {
		return true, nil
	}, "End", workflow.WithParallelCount(3))

	b.AddTimeout("Random", workflow.DurationTimerFunc[string, string](time.Hour), func(ctx context.Context, r *workflow.Record[string, string], now time.Time) (bool, error) {
		return true, nil
	}, "End")

	b.ConnectWorkflow(
		"other workflow",
		"Completed",
		memstreamer.New(),
		func(ctx context.Context, e *workflow.Event) (string, error) { return e.ForeignID, nil },
		func(ctx context.Context, r *workflow.Record[string, string], e *workflow.Event) (bool, error) {
			return true, nil
		},
		"End",
		workflow.WithParallelCount(2),
	)

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		timeoutStore,
		memrolescheduler.New(),
	)

	require.Equal(t, map[string]workflow.State{}, wf.States())

	ctx := context.Background()
	wf.Run(ctx)

	time.Sleep(time.Second)

	require.Equal(t, map[string]workflow.State{
		"example-middle-to-end-consumer-1-of-3":                             workflow.StateRunning,
		"example-middle-to-end-consumer-2-of-3":                             workflow.StateRunning,
		"example-middle-to-end-consumer-3-of-3":                             workflow.StateRunning,
		"example-start-to-middle-consumer-1-of-1":                           workflow.StateRunning,
		"example-random-timeout-auto-inserter-consumer":                     workflow.StateRunning,
		"example-random-timeout-consumer":                                   workflow.StateRunning,
		"other_workflow-completed-to-example-end-connector-consumer-1-of-2": workflow.StateRunning,
		"other_workflow-completed-to-example-end-connector-consumer-2-of-2": workflow.StateRunning,
	}, wf.States())

	wf.Stop()
	require.Equal(t, map[string]workflow.State{
		"example-start-to-middle-consumer-1-of-1":                           workflow.StateShutdown,
		"example-middle-to-end-consumer-1-of-3":                             workflow.StateShutdown,
		"example-middle-to-end-consumer-2-of-3":                             workflow.StateShutdown,
		"example-middle-to-end-consumer-3-of-3":                             workflow.StateShutdown,
		"example-random-timeout-auto-inserter-consumer":                     workflow.StateShutdown,
		"example-random-timeout-consumer":                                   workflow.StateShutdown,
		"other_workflow-completed-to-example-end-connector-consumer-1-of-2": workflow.StateShutdown,
		"other_workflow-completed-to-example-end-connector-consumer-2-of-2": workflow.StateShutdown,
	}, wf.States())
}

func TestConnectStream(t *testing.T) {
	ctx := context.Background()
	streamerA := memstreamer.New()

	type typeA struct {
		Val string
	}

	a := workflow.NewBuilder[typeA, string]("workflow A")
	a.AddStep("Begin", func(ctx context.Context, r *workflow.Record[typeA, string]) (bool, error) {
		r.Object.Val = "workflow A set this value"
		return true, nil
	}, "Completed")

	workflowA := a.Build(
		streamerA,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
	)
	workflowA.Run(ctx)

	streamerB := memstreamer.New()

	type typeB struct {
		Val string
	}
	b := workflow.NewBuilder[typeB, string]("workflow B")
	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[typeB, string]) (bool, error) {
		return true, nil
	}, "Middle")
	b.ConnectWorkflow(
		workflowA.Name,
		"Completed",
		streamerA,
		func(ctx context.Context, e *workflow.Event) (string, error) {
			return e.ForeignID, nil
		},
		func(ctx context.Context, r *workflow.Record[typeB, string], e *workflow.Event) (bool, error) {
			wr, err := workflow.UnmarshalRecord(e.Body)
			jtest.RequireNil(t, err)

			var objectA typeA
			err = workflow.Unmarshal(wr.Object, &objectA)
			jtest.RequireNil(t, err)

			// Copy the value over from objectA that came from workflowA to our object in workflowB
			r.Object.Val = objectA.Val

			return true, nil
		},
		"End",
	)

	workflowB := b.Build(
		streamerB,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
	)

	workflowB.Run(ctx)

	foreignID := "andrewwormald"

	// Start workflowB from "Start"
	runID, err := workflowB.Trigger(ctx, foreignID, "Start")
	jtest.RequireNil(t, err)

	// Wait until workflow B is at "Middle" where we expect workflowB to wait for workflowA
	_, err = workflowB.Await(ctx, foreignID, runID, "Middle")
	jtest.RequireNil(t, err)

	// Trigger workflowA
	_, err = workflowA.Trigger(ctx, foreignID, "Begin")
	jtest.RequireNil(t, err)

	// Wait until workflowB reaches "End" before finishing the test
	// After reaching "End" we know we merged an event from workflowA into workflowB in order for workflowB to complete.
	workflow.Require(t, workflowB, foreignID, runID, "End", typeB{
		Val: "workflow A set this value",
	})
}

func TestConnectStreamParallelConsumer(t *testing.T) {
	ctx := context.Background()
	streamerA := memstreamer.New()

	type typeA struct {
		Val string
	}

	a := workflow.NewBuilder[typeA, string]("workflow A")
	a.AddStep("Begin", func(ctx context.Context, r *workflow.Record[typeA, string]) (bool, error) {
		r.Object.Val = "workflow A set this value"
		return true, nil
	}, "Completed")

	workflowA := a.Build(
		streamerA,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
	)
	workflowA.Run(ctx)

	streamerB := memstreamer.New()

	type typeB struct {
		Val string
	}
	b := workflow.NewBuilder[typeB, string]("workflow B")
	b.AddStep("Start", func(ctx context.Context, r *workflow.Record[typeB, string]) (bool, error) {
		return true, nil
	}, "Middle")
	b.ConnectWorkflow(
		workflowA.Name,
		"Completed",
		streamerA,
		func(ctx context.Context, e *workflow.Event) (string, error) {
			return e.ForeignID, nil
		},
		func(ctx context.Context, r *workflow.Record[typeB, string], e *workflow.Event) (bool, error) {
			wr, err := workflow.UnmarshalRecord(e.Body)
			jtest.RequireNil(t, err)

			var objectA typeA
			err = workflow.Unmarshal(wr.Object, &objectA)
			jtest.RequireNil(t, err)

			// Copy the value over from objectA that came from workflowA to our object in workflowB
			r.Object.Val = objectA.Val

			return true, nil
		},
		"End",
		workflow.WithParallelCount(2),
	)

	workflowB := b.Build(
		streamerB,
		memrecordstore.New(),
		memtimeoutstore.New(),
		memrolescheduler.New(),
	)

	workflowB.Run(ctx)

	foreignID := "andrewwormald"

	// Start workflowB from "Start"
	runID, err := workflowB.Trigger(ctx, foreignID, "Start")
	jtest.RequireNil(t, err)

	// Wait until workflow B is at "Middle" where we expect workflowB to wait for workflowA
	_, err = workflowB.Await(ctx, foreignID, runID, "Middle")
	jtest.RequireNil(t, err)

	// Trigger workflowA
	_, err = workflowA.Trigger(ctx, foreignID, "Begin")
	jtest.RequireNil(t, err)

	// Wait until workflowB reaches "End" before finishing the test
	// After reaching "End" we know we merged an event from workflowA into workflowB in order for workflowB to complete.
	workflow.Require(t, workflowB, foreignID, runID, "End", typeB{
		Val: "workflow A set this value",
	})
}
