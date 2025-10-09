-- basic schemas, tables, and views
create schema basic;
set schema basic;

create table raw_customers (
  id int,
  name varchar(256),
  created_at timestamp
);

create view customers as
select
  id,
  name,
  created_at
from raw_customers;


-- comments
create schema comments;
set schema comments;

create table mytable (id int);
comment on table mytable is 'this is a table comment';
comment on column mytable.id is 'this is a column comment';

create view myview as select * from mytable;
comment on table myview is 'this is a view comment';
comment on column myview.id is 'this is a view column comment';


-- case-sensitive schema, table, and column names
create schema "case_sensitive";
set schema "case_sensitive";
create table "lowercase_table" (
  "id" int
);
create view "lowercase_view" as select * from "lowercase_table";


-- view with unqualified names belonging to a different schema
create schema qualifier_test;
create schema qualifier_target;
set schema qualifier_target;
create table mytable (
  id int
);
create view qualifier_test.myview as select * from mytable;


-- stored procs
create schema procedures;
create schema procedures_target;
set schema procedures_target;
create table source_table (id int);
create table target_table (id int);

create procedure procedures.myprocedure()
language sql
begin
  insert into target_table (id) select id from source_table;
end;
comment on procedure procedures.myprocedure is 'this is a procedure comment';