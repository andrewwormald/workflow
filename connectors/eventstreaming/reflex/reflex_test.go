package reflex_test

import (
	"github.com/luno/reflex/rpatterns"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/require"

	"github.com/andrewwormald/workflow/connectors/eventstreaming/reflex"
	connector "github.com/andrewwormald/workflow/connectors/testing"
)

func TestStreamer(t *testing.T) {
	eventsTable := rsql.NewEventsTable("my_events_table", rsql.WithEventMetadataField("metadata"), rsql.WithEventsBackoff(time.Millisecond*50))
	dbc := ConnectForTesting(t)
	constructor := reflex.New(dbc, dbc, eventsTable, rpatterns.MemCursorStore())
	connector.TestStreamer(t, constructor)
}

func TestParseStatus(t *testing.T) {
	e, err := reflex.TranslateToEventType("a", "b")
	jtest.RequireNil(t, err)
	require.Equal(t, 1294271946, e.ReflexType())
}
