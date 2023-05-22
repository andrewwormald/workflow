package sqlcursor_test

import (
	"database/sql"
	"testing"

	"github.com/corverroos/truss"
	_ "github.com/go-sql-driver/mysql"
)

var migrations = []string{`
	create table workflow_cursors (
		id            bigint not null auto_increment,
		name           varchar(255) not null,
		value         varchar(255) not null,
		created_at    datetime(3) not null,
		updated_at    datetime(3) not null,

		primary key(id),

		index by_key (name)
	);
`,
}

func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t, migrations...)
}
