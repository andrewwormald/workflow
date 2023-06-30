package workflow

import (
	"context"
	"io"
	clock_testing "k8s.io/utils/clock/testing"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testStatus string

func (ts testStatus) String() string {
	return string(ts)
}

func TestDetermineEndPoints(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"))
	b.AddStep(testStatus("Middle"), nil, testStatus("End"))
	wf := b.Build(context.Background())

	expected := map[string]bool{
		"End": true,
	}

	require.Equal(t, expected, wf.endPoints)
}

func TestWithStepErrBackOff(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"), WithStepErrBackOff(time.Minute))
	wf := b.Build(context.Background())

	require.Equal(t, time.Minute, wf.processes["Start"][0].ErrBackOff)
}

func TestWithStepPollingFrequency(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"), WithStepPollingFrequency(time.Minute))
	wf := b.Build(context.Background())

	require.Equal(t, time.Minute, wf.processes["Start"][0].PollingFrequency)
}

func TestStepDestinationStatus(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"))
	wf := b.Build(context.Background())

	require.Equal(t, "Middle", wf.processes["Start"][0].DestinationStatus.String())
}

func TestWithParallelCount(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"), WithParallelCount(100))
	wf := b.Build(context.Background())

	require.Equal(t, int64(100), wf.processes["Start"][0].ParallelCount)
}

func TestWithClock(t *testing.T) {
	now := time.Now()
	clock := clock_testing.NewFakeClock(now)
	b := BuildNew[string]("determine starting points", nil, nil)
	wf := b.Build(context.Background(), WithClock(clock))

	clock.Step(time.Hour)

	require.Equal(t, now.Add(time.Hour), wf.clock.Now())
}

func TestAddingCallbacks(t *testing.T) {
	var exampleFn CallbackFunc[string] = func(ctx context.Context, key Key, t *string, r io.Reader) (bool, error) {
		return true, nil
	}

	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddCallback(testStatus("Start"), exampleFn, testStatus("End"))
	wf := b.Build(context.Background())

	require.Equal(t, "End", wf.callback["Start"][0].DestinationStatus.String())
	require.NotNil(t, wf.callback["Start"][0].CallbackFunc)
}
