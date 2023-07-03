package workflow

import (
	"context"
	"io"
	clock_testing "k8s.io/utils/clock/testing"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDetermineEndPoints(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep("Start", nil, "Middle")
	b.AddStep("Middle", nil, "End")
	wf := b.Build()

	expected := map[string]bool{
		"End": true,
	}

	require.Equal(t, expected, wf.endPoints)
}

func TestWithStepErrBackOff(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep("Start", nil, "Middle", WithStepErrBackOff(time.Minute))
	wf := b.Build()

	require.Equal(t, time.Minute, wf.processes["Start"][0].ErrBackOff)
}

func TestWithStepPollingFrequency(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep("Start", nil, "Middle", WithStepPollingFrequency(time.Minute))
	wf := b.Build()

	require.Equal(t, time.Minute, wf.processes["Start"][0].PollingFrequency)
}

func TestStepDestinationStatus(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep("Start", nil, "Middle")
	wf := b.Build()

	require.Equal(t, "Middle", wf.processes["Start"][0].DestinationStatus)
}

func TestWithParallelCount(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep("Start", nil, "Middle", WithParallelCount(100))
	wf := b.Build()

	require.Equal(t, int64(100), wf.processes["Start"][0].ParallelCount)
}

func TestWithClock(t *testing.T) {
	now := time.Now()
	clock := clock_testing.NewFakeClock(now)
	b := BuildNew[string]("determine starting points", nil, nil)
	wf := b.Build(WithClock(clock))

	clock.Step(time.Hour)

	require.Equal(t, now.Add(time.Hour), wf.clock.Now())
}

func TestAddingCallbacks(t *testing.T) {
	var exampleFn CallbackFunc[string] = func(ctx context.Context, key Key, t *string, r io.Reader) (bool, error) {
		return true, nil
	}

	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddCallback("Start", exampleFn, "End")
	wf := b.Build()

	require.Equal(t, "End", wf.callback["Start"][0].DestinationStatus)
	require.NotNil(t, wf.callback["Start"][0].CallbackFunc)
}
