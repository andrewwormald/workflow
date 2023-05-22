package memcursor

import (
	"context"
	"sync"

	"github.com/luno/jettison/errors"

	"github.com/andrewwormald/workflow"
)

func New() *Cursor {
	return &Cursor{
		store: make(map[string]string),
	}
}

var _ workflow.Cursor = (*Cursor)(nil)

type Cursor struct {
	mu    sync.Mutex
	store map[string]string
}

func (c *Cursor) Get(ctx context.Context, name string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key, ok := c.store[name]
	if !ok {
		return "", errors.Wrap(workflow.ErrCursorNotFound, "")
	}

	return key, nil
}

func (c *Cursor) Set(ctx context.Context, name, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[name] = value
	return nil
}
