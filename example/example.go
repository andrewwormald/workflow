package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	clock_testing "k8s.io/utils/clock/testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memcursor"
	"github.com/andrewwormald/workflow/memstore"
)

func main() {
	store := memstore.New()
	cursor := memcursor.New()

	var sender Sender = func(ctx context.Context, email string, body io.Reader) error {
		return nil
	}

	var tradeCollector TradeCollector = func(ctx context.Context, email, currency string) ([]Trade, error) {
		return []Trade{
			{
				Volume: 100.68,
			},
			{
				Volume: 32.97,
			},
			{
				Volume: 87.35,
			},
		}, nil
	}

	ctx := context.Background()

	date := time.Date(2023, time.April, 9, 10, 30, 0, 0, time.UTC)
	fakeClock := clock_testing.NewFakeClock(date)

	wf := NewSendTradingSummaryWorkflow(store, cursor, sender, tradeCollector, fakeClock)

	// Calling run will invoke the workflow to start its "engine" aka consumers to start running. Run only needs to be
	// called one all further calls to Run will be no-ops.
	wf.Run(ctx)

	// Start interactive workflow
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter the customer's name: ")
	name, _ := reader.ReadString('\n')
	foreignID := name

	fmt.Print("Enter the customer's email: ")
	email, _ := reader.ReadString('\n')

	fmt.Print("Enter the FIAT currency for the report: ")
	currency, _ := reader.ReadString('\n')

	runID, err := wf.Trigger(
		ctx,
		foreignID,
		Started,
		workflow.WithInitialValue(&TradeSummary{
			Email:    strings.TrimSpace(email),
			Currency: strings.TrimSpace(currency),
		},
		))
	if err != nil {
		panic(err)
	}

	fmt.Print("Please enter the name of whom is reviewing the approval of this send: ")
	reviewedBy, _ := reader.ReadString('\n')

	fmt.Print("Please approve action to send trading summary: [y/n] ")
	yesOrNo, _ := reader.ReadString('\n')

	var canSend bool
	switch strings.TrimSpace(strings.ToLower(yesOrNo)) {
	case "y", "yes":
		canSend = true
	case "n", "no":
		canSend = false
	}

	resp := ApprovalResponse{
		Approved:   canSend,
		ReviewedBy: reviewedBy,
	}

	b, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}

	err = wf.Callback(ctx, foreignID, Started, bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

	// Wait for the workflow to get to the CollectedTrades state before changing the time
	_, err = wf.Await(ctx, foreignID, runID, CollectedTrades)
	if err != nil {
		panic(err)
	}

	oneDay := 24 * time.Hour
	// Advance 21 days from the 9th April into the new month to trigger the report sending (1st May)
	fakeClock.Step(22 * oneDay)

	fmt.Println("Speeding up time by 22 days...")

	tradeSummary, err := wf.Await(ctx, foreignID, runID, Sent)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Trade summary sent to %v with a recorded volume of %v (%v) \n", tradeSummary.Email, tradeSummary.TotalVolume, tradeSummary.Currency)

	fmt.Println("Speeding up time by 7 days...")

	// Update the clock by another week to request feedback
	time.Sleep(500 * time.Millisecond)
	fakeClock.Step(7 * oneDay)

	_, err = wf.Await(ctx, foreignID, runID, FeedbackRequested)
	if err != nil {
		panic(err)
	}
}
