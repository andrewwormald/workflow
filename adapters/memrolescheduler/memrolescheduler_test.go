package memrolescheduler_test

import (
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	adapterstesting "github.com/andrewwormald/workflow/adapters/testing"
	"testing"
)

func TestAwaitRoleContext(t *testing.T) {
	adapterstesting.TestRoleScheduler(t, func() workflow.RoleScheduler {
		return memrolescheduler.New()
	})
}
