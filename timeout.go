package workflow

import "time"

type Timeout struct {
	ID           int64
	RunID        string
	WorkflowName string
	ForeignID    string
	Status       string
	Completed    bool
	ExpireAt     time.Time
	CreatedAt    time.Time
}
