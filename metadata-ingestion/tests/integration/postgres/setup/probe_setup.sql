-- Seed for test_postgres_probe.py, run by that module's fixture after the
-- container starts. A database of its own, so the goldens built from
-- setup.sql never see it.
create database probetest;

\c probetest;

create schema sales;
-- Quoted, so Postgres keeps the mixed case rather than folding it.
create schema "MixedCase";
-- Denied by the parity recipe's schema_pattern.
create schema scratch;

create table sales.orders (id int primary key, customer_id int, amount numeric(10, 2));
create table sales.customers (id int primary key, name text);
-- Denied by the parity recipe's table_pattern.
create table sales.tmp_load (id int);
-- Read only by the probe gate test, which must never return its rows.
create table sales.secrets (id int, secret_value text);
insert into sales.secrets values (1, 'seeded-secret-value');

create view sales.big_orders as select id, amount from sales.orders where amount > 100;
-- Denied by the parity recipe's view_pattern.
create view sales.v_internal as select id from sales.customers;
create materialized view sales.order_totals as
  select customer_id, sum(amount) as total from sales.orders group by customer_id;

create table "MixedCase"."CamelOrders" ("OrderId" int, "Amount" int);

create table scratch.junk (id int);
