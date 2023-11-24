package reflex_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/corverroos/truss"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex/rpatterns"
	"github.com/luno/reflex/rsql"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/examples/gettingstarted"
	"github.com/andrewwormald/workflow/examples/reflex"
)

func TestReflexExampleWorkflow(t *testing.T) {
	dbc := ConnectForTesting(t)

	table := rsql.NewEventsTable("my_events_table", rsql.WithEventMetadataField("metadata"))
	wf := reflex.ReflexExampleWorkflow(dbc, table, rpatterns.MemCursorStore())
	t.Cleanup(wf.Stop)

	ctx := context.Background()
	wf.Run(ctx)

	foreignID := "82347982374982374"
	runID, err := wf.Trigger(ctx, foreignID, gettingstarted.StatusStarted)
	jtest.RequireNil(t, err)

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusReadTheDocs, gettingstarted.GettingStarted{
		ReadTheDocs: "✅",
	})

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusFollowedTheExample, gettingstarted.GettingStarted{
		ReadTheDocs:     "✅",
		FollowAnExample: "✅",
	})

	workflow.Require(t, wf, foreignID, runID, gettingstarted.StatusCreatedAFunExample, gettingstarted.GettingStarted{
		ReadTheDocs:       "✅",
		FollowAnExample:   "✅",
		CreateAFunExample: "✅",
	})
}

var tables = []string{
	`
	create table my_events_table (
	  id bigint not null auto_increment,
	  foreign_id varchar(255) not null,
	  timestamp datetime not null,
	  type bigint not null default 0,
	  metadata blob,
	  
  	  primary key (id)
	);
`,
}

// ConnectForTesting returns a database connection for a temp database with latest schema.
func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t, tables...)
}
