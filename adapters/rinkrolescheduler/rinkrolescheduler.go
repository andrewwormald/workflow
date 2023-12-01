package rinkrolescheduler

import (
	"context"
	"fmt"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/rink/v2"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/andrewwormald/workflow"
)

func New(client *clientv3.Client, name string, opts ...rink.Option) workflow.RoleScheduler {
	return &impl{
		client: client,
		name:   name,
		opts:   opts,
	}
}

type impl struct {
	client *clientv3.Client
	name   string
	opts   []rink.Option

	shutdowns []func(ctx context.Context)
}

func (i *impl) Await(ctx context.Context, role string) (context.Context, context.CancelFunc, error) {
	cl := rink.New(i.client, i.name, i.opts...)
	i.shutdowns = append(i.shutdowns, cl.Shutdown)

	return cl.Roles.AwaitRoleContext(ctx, role)
}

func (i *impl) Stop(ctx context.Context) {
	fmt.Println("called stop ", len(i.shutdowns))
	for _, shutdown := range i.shutdowns {
		shutdown(ctx)
	}

	err := i.client.Close()
	if err != nil {
		log.Error(ctx, errors.Wrap(err, "failed to close etc client"))
	}
	fmt.Println("stopped")
}
