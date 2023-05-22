package sqlcursor

import "time"

type CursorEntry struct {
	ID        int64
	Name      string
	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}
