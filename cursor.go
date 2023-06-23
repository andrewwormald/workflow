package workflow

import (
	"context"
	"fmt"
)

type Cursor interface {
	Get(ctx context.Context, name string) (string, error)
	Set(ctx context.Context, name, value string) error
}

func cursorName(workflowName string, status string, shard, totalShards int64) string {
	return fmt.Sprintf("%v-%v-%v-of-%v", workflowName, status, shard, totalShards)
}
