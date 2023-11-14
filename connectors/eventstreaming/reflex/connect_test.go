package reflex_test

import (
	"database/sql"
	"github.com/corverroos/truss"
	"testing"
)

var tables = []string{
	`
	create table my_events_table (
	  id bigint not null auto_increment,
	  foreign_id varchar(255) not null,
	  timestamp datetime not null,
	  type bigint not null default 0,
	  metadata blob,
	  
  	  primary key (id)
	);
`,
}

// ConnectForTesting returns a database connection for a temp database with latest schema.
func ConnectForTesting(t *testing.T) *sql.DB {
	return truss.ConnectForTesting(t, tables...)
}
