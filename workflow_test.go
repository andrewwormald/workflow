package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/andrewwormald/workflow/workflowtest"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"io"
	"strconv"
	"testing"

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

// CheckStatus is the state of an Onfido check.
type Status int

func (s Status) DynaFlowStep() int {
	return int(s)
}

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
	StatusCompleted                Status = 8
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

type Backends struct {
	cursor workflow.Cursor
	store  workflow.Store
}

func (b *Backends) WorkflowStore() workflow.Store {
	return b.store
}

func (b *Backends) WorkflowCursor() workflow.Cursor {
	return b.cursor
}

func TestWorkflow(t *testing.T) {
	var (
		expectedUserID      int64 = 984892374983743
		expectedFinalStatus       = StatusCompleted
		expectedProfile           = "Andrew Wormald"
		expectedEmail             = "andreww@luno.com"
		expectedCellphone         = "+44 7467623292"
		expectedOTP               = 345345
		expectedOTPVerified       = true
	)

	createProfile := func(mt *MyType) (bool, error) {
		mt.Profile = "Andrew Wormald"
		fmt.Println("creating profile", *mt)
		return true, nil
	}

	sendEmailConfirmation := func(mt *MyType) (bool, error) {
		fmt.Println("sending email confirmation", *mt)
		return true, nil
	}

	emailVerifiedCallback := func(mt *MyType, r io.Reader) (bool, error) {
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

	cellphoneNumberCallback := func(mt *MyType, r io.Reader) (bool, error) {
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

	sendOTP := func(mt *MyType) (bool, error) {
		fmt.Println("send otp", *mt)
		mt.OTP = expectedOTP
		return true, nil
	}

	otpCallback := func(mt *MyType, r io.Reader) (bool, error) {
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

	sendTermsAndConditions := func(mt *MyType) (bool, error) {
		fmt.Println("send terms and conditions", *mt)
		return true, nil
	}

	backends := &Backends{
		cursor: memcursor.New(),
		store:  memstore.New(),
	}
	// This introduces a validation element to the producing of events.
	w := workflow.New("sign up",
		backends,
		workflow.StatefulStep(StatusInitiated, createProfile, StatusProfileCreated),
		workflow.StatefulStep(StatusProfileCreated, sendEmailConfirmation, StatusEmailConfirmationSent),
		workflow.Callback(StatusEmailConfirmationSent, emailVerifiedCallback, StatusEmailVerified),
		workflow.Callback(StatusEmailVerified, cellphoneNumberCallback, StatusCellphoneNumberSubmitted),
		workflow.StatefulStep(StatusCellphoneNumberSubmitted, sendOTP, StatusOTPSent),
		workflow.Callback(StatusOTPSent, otpCallback, StatusOTPVerified),
		workflow.StatefulStep(StatusOTPVerified, sendTermsAndConditions, StatusCompleted),
	)

	ctx := context.Background()
	fid := strconv.FormatInt(expectedUserID, 10)

	w.LaunchProcesses(ctx)

	mt := MyType{
		UserID: expectedUserID,
	}
	err := w.Trigger(ctx, fid, StatusInitiated, workflow.WithInitialValue(&mt))
	jtest.RequireNil(t, err)

	// Once in the correct state, trigger third party callbacks
	workflowtest.TriggerCallbackOn(t, w, fid, StatusEmailConfirmationSent, ExternalEmailVerified{
		IsVerified: true,
	})
	workflowtest.TriggerCallbackOn(t, w, fid, StatusEmailVerified, ExternalCellPhoneSubmitted{
		DialingCode: "+44",
		Number:      "7467623292",
	})
	workflowtest.TriggerCallbackOn(t, w, fid, StatusOTPSent, ExternalOTP{
		OTPCode: expectedOTP,
	})

	_, err = w.Await(ctx, fid, StatusCompleted)
	jtest.RequireNil(t, err)

	r, err := backends.store.LookupLatest(ctx, "sign up", fid)
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
