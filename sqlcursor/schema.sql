-- Example of required schema. Name of the table can be different and must match name provided to sqlcursor.new("{{table_name}}"...)
create table workflow_cursors (
    id            bigint not null auto_increment,
    name           varchar(255) not null,
    value         varchar(255) not null,
    created_at    datetime(3) not null,
    updated_at    datetime(3) not null,

    primary key(id),

    index by_key (name)
);