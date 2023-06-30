package sqlstore_test

import (
	"testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/sqlstore"
	"github.com/andrewwormald/workflow/storetesting"
)

func TestStore(t *testing.T) {
	storetesting.TestStore(t, func() workflow.Store {
		dbc := ConnectForTesting(t)
		return sqlstore.New("workflow_records", "workflow_timeouts", dbc, dbc)
	})
}
