package reflex

import (
	"database/sql"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
	"github.com/andrewwormald/workflow/adapters/reflexstreamer"
	"github.com/andrewwormald/workflow/examples/gettingstarted"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

func ReflexExampleWorkflow(db *sql.DB, table *rsql.EventsTable, cstore reflex.CursorStore) *workflow.Workflow[gettingstarted.GettingStarted, gettingstarted.Status] {
	return gettingstarted.GettingStartedWithEnumWorkflow(gettingstarted.Deps{
		EventStreamer: reflexstreamer.New(db, db, table, cstore),
		RecordStore:   memrecordstore.New(),
		TimeoutStore:  memtimeoutstore.New(),
		RoleScheduler: memrolescheduler.New(),
	})
}
