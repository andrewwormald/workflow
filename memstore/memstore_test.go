package memstore_test

import (
	"testing"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memstore"
	"github.com/andrewwormald/workflow/storetesting"
)

func TestStore(t *testing.T) {
	storetesting.TestStore(t, func() workflow.Store {
		return memstore.New()
	})
}
