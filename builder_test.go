package workflow

import (
	"context"
	"io"
	clock_testing "k8s.io/utils/clock/testing"
	"reflect"
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

	require.Equal(t, time.Minute, wf.consumers["Start"][0].ErrBackOff)
}

func TestStepDestinationStatus(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle")
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, "Middle", wf.consumers["Start"][0].DestinationStatus)
}

func TestWithParallelCount(t *testing.T) {
	b := NewBuilder[string, string]("determine starting points")
	b.AddStep("Start", nil, "Middle", WithParallelCount(100))
	wf := b.Build(nil, nil, nil, nil)

	require.Equal(t, int(100), wf.consumers["Start"][0].ParallelCount)
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

func TestConnectWorkflowConstruction(t *testing.T) {
	var externalStream = (EventStreamer)(nil)

	b := NewBuilder[string, string]("workflow B")
	b.AddStep("Start", func(ctx context.Context, r *Record[string, string]) (bool, error) {
		return true, nil
	}, "Middle")

	filter := func(ctx context.Context, e *Event) (string, error) {
		return e.ForeignID, nil
	}

	consumer := func(ctx context.Context, r *Record[string, string], e *Event) (bool, error) {
		return true, nil
	}
	b.ConnectWorkflow(
		"workflowA",
		"Completed",
		externalStream,
		filter,
		consumer,
		"End",
		WithParallelCount(3),
		WithStepPollingFrequency(time.Second*10),
		WithStepErrBackOff(time.Minute),
	)
	wf := b.Build(nil, nil, nil, nil)

	for _, config := range wf.connectorConfigs {
		require.Equal(t, "workflowA", config.workflowName)
		require.Equal(t, "Completed", config.status)
		require.Equal(t, externalStream, config.stream)
		require.Equal(t, reflect.ValueOf(consumer).Pointer(), reflect.ValueOf(config.consumer).Pointer())
		require.Equal(t, "End", config.to)
		require.Equal(t, time.Second*10, config.pollingFrequency)
		require.Equal(t, time.Minute, config.errBackOff)
		require.Equal(t, 3, config.parallelCount)
	}
}
