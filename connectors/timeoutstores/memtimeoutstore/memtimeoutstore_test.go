package memtimeoutstore_test

import (
	"testing"

	"github.com/andrewwormald/workflow"
	connectortesting "github.com/andrewwormald/workflow/connectors/testing"
	"github.com/andrewwormald/workflow/connectors/timeoutstores/memtimeoutstore"
)

func TestStore(t *testing.T) {
	connectortesting.TestTimeoutStore(t, func() workflow.TimeoutStore {
		return memtimeoutstore.New()
	})
}
