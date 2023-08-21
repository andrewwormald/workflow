package workflow

import (
	"context"
	"encoding/json"
	"net/http"
)

func newInternalState() *internalState {
	return &internalState{}
}

type internalState struct {
	runners []RunnerState
}

func (is *internalState) Add(state RunnerState) {
	is.runners = append(is.runners, state)
}

func (is *internalState) RegisterHTTPHandler(ctx context.Context) {
	http.Handle("workflow/internal/state/runners/list", listRunners(is))
}

func listRunners(is *internalState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := json.Marshal(is.runners)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}

		_, err = w.Write(b)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.WriteHeader(200)
	}
}

type RunnerState struct {
	Name   string
	Status runnerStatus
}

type runnerStatus string

const (
	unknown runnerStatus = "Unknown"
	running runnerStatus = "Running"
	waiting runnerStatus = "Waiting"
	offline runnerStatus = "Offline"
)
