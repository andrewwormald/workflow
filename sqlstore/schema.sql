-- Example of required schema. Name of the table can be different and must match name provided to sqlstore.new("{{table_name}}"...)
create table workflow_entries (
    id                     bigint not null auto_increment,
    workflow_name           varchar(255) not null,
    foreign_id             varchar(255) not null,
    status                 varchar(255) not null,
    object                 blob not null,
    created_at             datetime(3) not null,

    primary key(id),

    index by_workflow_name_foreign_id (workflow_name, foreign_id),
    index by_workflow_name_status (workflow_name, status),
    index by_workflow_name_foreign_id_status (workflow_name, foreign_id, status)
);