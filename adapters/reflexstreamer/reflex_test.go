package reflexstreamer_test

import (
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex/rpatterns"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/require"

	"github.com/andrewwormald/workflow/adapters/reflexstreamer"
	adapter "github.com/andrewwormald/workflow/adapters/testing"
)

func TestStreamer(t *testing.T) {
	eventsTable := rsql.NewEventsTable("my_events_table", rsql.WithEventMetadataField("metadata"))
	dbc := ConnectForTesting(t)
	constructor := reflexstreamer.New(dbc, dbc, eventsTable, rpatterns.MemCursorStore())
	adapter.TestStreamer(t, constructor)
}

func TestParseStatus(t *testing.T) {
	e, err := reflexstreamer.TranslateToEventType("a", "b")
	jtest.RequireNil(t, err)
	require.Equal(t, 1294271946, e.ReflexType())
}
