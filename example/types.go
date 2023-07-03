package main

import (
	"context"
	"io"
)

type (
	Usage struct{}
	Trade struct {
		Volume float64
	}

	TradeCollector func(ctx context.Context, email string, currency string) ([]Trade, error)
	Sender         func(ctx context.Context, email string, body io.Reader) error

	TradeSummary struct {
		Email            string
		Trades           []Trade
		Currency         string
		TotalVolume      float64
		ApprovalResponse ApprovalResponse
	}

	ApprovalResponse struct {
		Approved   bool
		ReviewedBy string
	}
)
