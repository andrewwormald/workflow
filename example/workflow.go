package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/utils/clock"
	"time"

	"github.com/andrewwormald/workflow"
)

var (
	Started           = "Started"
	CollectedTrades   = "Collected trades"
	Reviewed          = "Request reviewed"
	Accepted          = "Accepted"
	Rejected          = "Rejected"
	Sent              = "Sent"
	FeedbackRequested = "Sent feedback request"
)

func NewSendTradingSummaryWorkflow(store workflow.Store, cursor workflow.Cursor, sender Sender, tradeCollector TradeCollector, clock clock.Clock) *workflow.Workflow[TradeSummary] {
	b := workflow.BuildNew[TradeSummary]("user account report", store, cursor)

	now := clock.Now()
	reportSendDate := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	b.AddCallback(
		Started,
		func(ctx context.Context, key workflow.Key, t *TradeSummary, r io.Reader) (bool, error) {
			b, err := io.ReadAll(r)
			if err != nil {
				return false, err
			}

			var resp ApprovalResponse
			err = json.Unmarshal(b, &resp)
			if err != nil {
				return false, err
			}

			fmt.Println("Workflow: Trading summary request reviewed")

			t.ApprovalResponse = resp

			return true, nil
		},
		Reviewed,
	)

	b.AddStep(
		Reviewed,
		func(ctx context.Context, key workflow.Key, t *TradeSummary) (bool, error) {
			// Transition to StatusTradeSummaryRejected when its is not approved after review
			return !t.ApprovalResponse.Approved, nil
		},
		Rejected,
	)

	b.AddStep(
		Reviewed,
		func(ctx context.Context, key workflow.Key, t *TradeSummary) (bool, error) {
			// Transition to StatusTradeSummaryRejected when its is not approved after review
			return t.ApprovalResponse.Approved, nil
		},
		Accepted,
	)

	b.AddStep(
		Accepted,
		func(ctx context.Context, key workflow.Key, t *TradeSummary) (bool, error) {
			trades, err := tradeCollector(ctx, t.Email, t.Currency)
			if err != nil {
				return false, err
			}

			fmt.Println("Workflow: Collected trades")

			var total float64
			for _, trade := range trades {
				total += trade.Volume
			}

			t.Trades = trades
			t.TotalVolume = total
			return true, nil
		},
		CollectedTrades,
	)

	b.AddTimeout(
		CollectedTrades,
		func(ctx context.Context, key workflow.Key, t *TradeSummary, now time.Time) (bool, error) {
			msg := fmt.Sprintf("Your total trading volumne was %v (%v)", t.TotalVolume, t.Currency)
			err := sender(ctx, t.Email, bytes.NewReader([]byte(msg)))
			if err != nil {
				return false, err
			}

			fmt.Println("Workflow: Trading summary sent")

			return true, nil
		},
		reportSendDate,
		Sent,
	)

	oneDay := 24 * time.Hour
	oneWeek := 7 * oneDay

	b.AddTimeoutWithDuration(
		Sent,
		func(ctx context.Context, key workflow.Key, t *TradeSummary, now time.Time) (bool, error) {
			fmt.Printf("Workflow: Requesting feedback from customer %v \n", t.Email)
			return true, nil
		},
		oneWeek,
		FeedbackRequested,
	)

	return b.Build(workflow.WithClock(clock))
}
