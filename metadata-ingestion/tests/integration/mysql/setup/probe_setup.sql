-- Seed for test_mysql_probe.py, run by that module's fixture after the
-- container starts. Databases of its own, so the goldens built from
-- setup.sql never see them.
CREATE DATABASE probe_sales;
-- Denied by the parity recipe's database_pattern.
CREATE DATABASE probe_scratch;

USE probe_sales;

CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10, 2));
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50));
-- Denied by the parity recipe's table_pattern.
CREATE TABLE tmp_load (id INT);
-- Read only by the probe gate test, which must never return its rows.
CREATE TABLE secrets (id INT, secret_value VARCHAR(50));
INSERT INTO secrets VALUES (1, 'seeded-secret-value');
-- Kept as written where lower_case_table_names is 0, folded where it is not.
CREATE TABLE `CamelOrders` (`OrderId` INT, `Amount` INT);

CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100;
-- Denied by the parity recipe's view_pattern.
CREATE VIEW v_internal AS SELECT id FROM customers;

CREATE TABLE probe_scratch.junk (id INT);
