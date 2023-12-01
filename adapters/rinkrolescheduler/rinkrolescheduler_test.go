package rinkrolescheduler_test

import (
	"github.com/luno/rink/v2"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/rinkrolescheduler"
	adapterstesting "github.com/andrewwormald/workflow/adapters/testing"
)

func TestRinkRoleScheduler(t *testing.T) {
	adapterstesting.TestRoleScheduler(t, func() workflow.RoleScheduler {
		cfg := clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: time.Second,
		}

		client, err := clientv3.New(cfg)
		jtest.RequireNil(t, err)

		return rinkrolescheduler.New(client, "test", rink.WithRolesOptions(rink.RolesOptions{
			Assign: func(role string, roleCount int32) int32 {
				return 0
			},
		}))
	})
}
