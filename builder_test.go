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
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle")
	b.AddStep("Middle", nil, "End")
	wf := b.Build(nil, nil, nil, nil)

	expected := map[string]bool{
		"End": true,
	}

	require.Equal(t, expected, wf.endPoints)
}

func TestWithStepErrBackOff(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle", WithStepErrBackOff(time.Minute))
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, time.Minute, wf.processes["Start"][0].ErrBackOff)
}

func TestStepDestinationStatus(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle")
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, "Middle", wf.processes["Start"][0].DestinationStatus)
}

func TestWithParallelCount(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle", WithParallelCount(100))
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, int64(100), wf.processes["Start"][0].ParallelCount)
}

func TestWithClock(t *testing.T) {
	now := time.Now()
	clock := clock_testing.NewFakeClock(now)
	b := NewBuilder[string, string]("determine starting points")
	wf := b.Build(nil, nil, nil, nil, WithClock(clock))

	clock.Step(time.Hour)

	require.Equal(t, now.Add(time.Hour), wf.clock.Now())
}

func TestAddingCallbacks(t *testing.T) {
	var exampleFn CallbackFunc[string, string] = func(ctx context.Context, s *Record[string, string], r io.Reader) (bool, error) {
		return true, nil
	}

	b := NewBuilder[string, string]("determine starting points")
	b.AddCallback("Start", exampleFn, "End")
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, "End", wf.callback["Start"][0].DestinationStatus)
	require.NotNil(t, wf.callback["Start"][0].CallbackFunc)
}

func TestWithTimeoutErrBackOff(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddTimeout(
		"Start",
		DurationTimerFunc[string, string](time.Hour),
		func(ctx context.Context, t *Record[string, string], now time.Time) (bool, error) {
			return true, nil
		},
		"End",
		WithTimeoutErrBackOff(time.Minute),
	)
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, time.Minute, wf.timeouts["Start"].ErrBackOff)
}

func TestWithTimeoutPollingFrequency(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddTimeout(
		"Start",
		DurationTimerFunc[string, string](time.Hour),
		func(ctx context.Context, t *Record[string, string], now time.Time) (bool, error) {
			return true, nil
		},
		"End",
		WithTimeoutPollingFrequency(time.Minute),
	)
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, time.Minute, wf.timeouts["Start"].PollingFrequency)
}
