package sqlstore_test

//import (
//	"testing"
//
//	"github.com/andrewwormald/workflow"
//	"github.com/andrewwormald/workflow/connectors/recordstores/sqlstore"
//	connectorstesting "github.com/andrewwormald/workflow/connectors/testing"
//)
//
//func TestStore(t *testing.T) {
//	connectorstesting.TestRecordStore(t, func() workflow.RecordStore {
//		dbc := ConnectForTesting(t)
//		return sqlstore.New("workflow_records", "workflow_timeouts", dbc, dbc)
//	})
//}
