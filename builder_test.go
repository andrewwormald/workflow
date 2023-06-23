package workflow

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type testStatus string

func (ts testStatus) String() string {
	return string(ts)
}

func TestDetermineStartingPoints(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"))
	b.AddStep(testStatus("Middle"), nil, testStatus("End"))
	wf := b.Complete()

	expected := map[string]bool{
		"Start": true,
	}

	require.Equal(t, expected, wf.startingPoints)
}

func TestDetermineEndPoints(t *testing.T) {
	b := BuildNew[string]("determine starting points", nil, nil)
	b.AddStep(testStatus("Start"), nil, testStatus("Middle"))
	b.AddStep(testStatus("Middle"), nil, testStatus("End"))
	wf := b.Complete()

	expected := map[string]bool{
		"End": true,
	}

	require.Equal(t, expected, wf.endPoints)
}
