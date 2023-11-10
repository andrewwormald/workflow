package memrecordstore_test

import (
	"testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/connectors/recordstores/memrecordstore"
	connectortesting "github.com/andrewwormald/workflow/connectors/testing"
)

func TestStore(t *testing.T) {
	connectortesting.TestRecordStore(t, func() workflow.RecordStore {
		return memrecordstore.New()
	})
}
