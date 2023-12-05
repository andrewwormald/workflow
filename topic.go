package workflow

import (
	"fmt"
	"strings"
)

func Topic(workflowName string, statusType int) string {
	name := strings.ReplaceAll(workflowName, " ", "-")
	return strings.Join([]string{
		name,
		fmt.Sprintf("%v", statusType),
	}, "-")
}
