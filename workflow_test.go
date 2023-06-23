package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memcursor"
	"github.com/andrewwormald/workflow/memstore"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"io"
	"strconv"
	"testing"
	"time"
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

type Status int

func (s Status) String() string {
	switch s {
	case StatusInitiated:
		return "Initiated"
	case StatusProfileCreated:
		return "Profile Created"
	case StatusEmailConfirmationSent:
		return "Email Confirmation Sent"
	case StatusEmailVerified:
		return "Email Verified"
	case StatusCellphoneNumberSubmitted:
		return "Cellphone Number Submitted"
	case StatusOTPSent:
		return "OTP Sent"
	case StatusOTPVerified:
		return "OTP Verified"
	case StatusCompleted:
		return "Completed"
	default:
		return "Unknown"
	}
}

const (
	StatusUnknown                  Status = 0
	StatusInitiated                Status = 1
	StatusProfileCreated           Status = 2
	StatusEmailConfirmationSent    Status = 3
	StatusEmailVerified            Status = 4
	StatusCellphoneNumberSubmitted Status = 5
	StatusOTPSent                  Status = 6
	StatusOTPVerified              Status = 7
	StatusCompleted                Status = 9
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
	store := memstore.New()
	cursor := memcursor.New()

	b := workflow.BuildNew[MyType]("user sign up", store, cursor)
	b.AddStep(StatusInitiated, createProfile, StatusProfileCreated)
	b.AddStep(StatusProfileCreated, sendEmailConfirmation, StatusEmailConfirmationSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusEmailConfirmationSent, emailVerifiedCallback, StatusEmailVerified)
	b.AddCallback(StatusEmailVerified, cellphoneNumberCallback, StatusCellphoneNumberSubmitted)
	b.AddStep(StatusCellphoneNumberSubmitted, sendOTP, StatusOTPSent, workflow.WithParallelCount(5))
	b.AddCallback(StatusOTPSent, otpCallback, StatusOTPVerified)
	b.AddTimeout(StatusOTPVerified, waitForAccountCoolDown, time.Hour, StatusCompleted)
	wf := b.Complete()

	ctx := context.Background()
	fid := strconv.FormatInt(expectedUserID, 10)

	wf.RunBackground(ctx)

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

	workflow.ChangeTimeOn(t, wf, fid, runID, StatusOTPVerified, time.Now().Add(time.Hour))

	_, err = wf.Await(ctx, fid, runID, StatusCompleted)
	jtest.RequireNil(t, err)

	key := workflow.MakeKey("user sign up", fid, runID)
	r, err := store.LookupLatest(ctx, key)
	jtest.RequireNil(t, err)
	require.Equal(t, expectedFinalStatus.String(), r.Status)

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
