package sqlstore_test

import (
	"database/sql"
	"testing"

	"github.com/corverroos/truss"
	_ "github.com/go-sql-driver/mysql"
)

var migrations = []string{`
	create table workflow_records (
		id                     bigint not null auto_increment,
		workflow_name           varchar(255) not null,
		foreign_id             varchar(255) not null,
		run_id                 varchar(255) not null,
		status                 varchar(255) not null,
		object                 blob not null,
		is_start               bool not null,
		is_end                 bool not null,
		created_at             datetime(3) not null,

		primary key(id),

		index by_workflow_name_status (workflow_name, status),
		index by_workflow_name_foreign_id_run_id (workflow_name, foreign_id, run_id)
	)`,
	`create table workflow_timeouts (
		id                     bigint not null auto_increment,
		workflow_name           varchar(255) not null,
		foreign_id             varchar(255) not null,
		run_id                 varchar(255) not null,
		status                 varchar(255) not null,
		completed              bool not null default false,
		expire_at              datetime(3) not null,
		created_at             datetime(3) not null,
	
		primary key(id),
	
		index by_completed_expire_at (completed, expire_at),
		index by_workflow_name_status (workflow_name, status)
	)`,
}

func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t, migrations...)
}
