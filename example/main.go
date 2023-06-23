package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/andrewwormald/workflow/memcursor"
	"github.com/andrewwormald/workflow/memstore"
	"io"

	"github.com/andrewwormald/workflow"
)

func main() {
	w := RegisterForEvent(memstore.New(), memcursor.New())
	ctx := context.Background()
	w.Run(ctx)

	foreignID := "Andrew Wormald"
	err := w.Trigger(ctx, foreignID, StatusStarted, workflow.WithInitialValue(&MyType{Email: "andrew@wormald.com"}))
	if err != nil {
		panic(err)
	}

	_, err = w.Await(ctx, foreignID, StatusEmailConfirmationSent)
	if err != nil {
		panic(err)
	}

	ecr := EmailConfirmationResponse{
		Confirmed: true,
	}

	b, err := json.Marshal(&ecr)
	if err != nil {
		panic(err)
	}

	// Imitate callback
	err = w.Callback(ctx, foreignID, StatusEmailConfirmationSent, bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

	mt, err := w.Await(ctx, foreignID, StatusNotificationEmailSent)
	if err != nil {
		panic(err)
	}

	fmt.Println("Completed workflow: ", mt)
}

type MyType struct {
	Email          string
	EmailConfirmed bool
	BookingUID     string
}

type Status string

func (s Status) String() string {
	return string(s)
}

var (
	StatusStarted               Status = "Started"
	StatusEmailConfirmationSent Status = "Email confirmation sent"
	StatusEmailConfirmed        Status = "Email confirmed"
	StatusBookingMade           Status = "Booking made"
	StatusNotificationEmailSent Status = "Notification email sent"
)

// RegisterForEvent is an example workflow that displays some core features of the workflow package
func RegisterForEvent(store workflow.Store, cursor workflow.Cursor) *workflow.Workflow[MyType] {
	return workflow.New(
		"event registration",
		store,
		cursor,
		// Send email confirmation
		workflow.Consume(StatusStarted, SendEmailConfirmation(), StatusEmailConfirmationSent),
		// Allow a callback for intake of email confirmation
		workflow.Callback(StatusEmailConfirmationSent, EmailConfirmed(), StatusEmailConfirmed),
		// Book the requested spot
		workflow.Consume(StatusEmailConfirmed, BookSlotForEvent(), StatusBookingMade),
		// Notify the customer that the booking was made
		workflow.Consume(StatusBookingMade, NotifySuccessfulBooking(), StatusNotificationEmailSent),
	)
}

func SendEmailConfirmation() func(ctx context.Context, mt *MyType) (bool, error) {
	return func(ctx context.Context, mt *MyType) (bool, error) {
		fmt.Println("Sending email to: ", mt.Email)
		return true, nil
	}
}

type EmailConfirmationResponse struct {
	Confirmed bool
}

func EmailConfirmed() func(ctx context.Context, mt *MyType, r io.Reader) (bool, error) {
	return func(ctx context.Context, mt *MyType, r io.Reader) (bool, error) {
		b, err := io.ReadAll(r)
		if err != nil {
			return false, nil
		}

		var ecr EmailConfirmationResponse
		err = json.Unmarshal(b, &ecr)
		if err != nil {
			return false, nil
		}

		mt.EmailConfirmed = ecr.Confirmed
		fmt.Println("Email confirmed: ", mt.EmailConfirmed)
		return true, nil
	}
}

func BookSlotForEvent() func(ctx context.Context, mt *MyType) (bool, error) {
	return func(ctx context.Context, mt *MyType) (bool, error) {
		mt.BookingUID = "UASDF798SD6F3BNSKADF"
		fmt.Println(fmt.Sprintf("Booking slot for event for: %v (Booking Ref: %v)", mt.Email, mt.BookingUID))
		return true, nil
	}
}

func NotifySuccessfulBooking() func(ctx context.Context, mt *MyType) (bool, error) {
	return func(ctx context.Context, mt *MyType) (bool, error) {
		mt.BookingUID = "UASDF798SD6F3BNSKADF"
		msg := fmt.Sprintf(` Hello %v,

		Your booking for the event has been confirmed.

		Your booking reference is: %v
		`, mt.Email, mt.BookingUID)

		fmt.Println(msg)
		return true, nil
	}
}
