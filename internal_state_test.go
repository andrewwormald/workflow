package workflow

import (
	"encoding/json"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalStateHandler(t *testing.T) {
	is := newInternalState()

	is.Add(RunnerState{
		Name:   "consumer 1",
		Status: running,
	})

	is.Add(RunnerState{
		Name:   "consumer 2",
		Status: waiting,
	})

	is.Add(RunnerState{
		Name:   "consumer 3",
		Status: offline,
	})

	srv := httptest.NewServer(listRunners(is))
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	jtest.RequireNil(t, err)

	client := http.Client{}
	res, err := client.Do(req)
	jtest.RequireNil(t, err)

	respBody, err := io.ReadAll(res.Body)
	jtest.RequireNil(t, err)

	expected := []RunnerState{
		{
			Name:   "consumer 1",
			Status: running,
		},
		{
			Name:   "consumer 2",
			Status: waiting,
		},
		{
			Name:   "consumer 3",
			Status: offline,
		},
	}

	expectedBytes, err := json.Marshal(expected)
	jtest.RequireNil(t, err)

	require.Equal(t, string(expectedBytes), string(respBody))
}
