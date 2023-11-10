package workflow

import "time"

type Timeout struct {
	ID           int64
	WorkflowName string
	ForeignID    string
	RunID        string
	Status       string
	Completed    bool
	ExpireAt     time.Time
	CreatedAt    time.Time
}
