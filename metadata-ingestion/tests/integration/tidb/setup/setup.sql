-- TiDB integration test fixture.
-- Intentionally avoids stored procedures / functions / triggers, which TiDB
-- does not support (see https://docs.pingcap.com/tidb/stable/mysql-compatibility/).
-- Exercises the metadata paths the TiDB source actually emits: tables with a
-- primary key, table/column comments, a foreign key, and a view (for view
-- definition + SQL-parsed lineage).

CREATE DATABASE IF NOT EXISTS datahub_test;
USE datahub_test;

CREATE TABLE customers (
  id            INT          NOT NULL,
  company       VARCHAR(50)  NULL COMMENT 'Customer company name',
  email_address VARCHAR(100) NULL,
  PRIMARY KEY (id)
) COMMENT = 'Customers of the business';

CREATE TABLE orders (
  id          INT NOT NULL,
  customer_id INT NOT NULL,
  amount      DECIMAL(10, 2) NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers (id)
) COMMENT = 'Orders placed by customers';

CREATE VIEW customer_orders AS
  SELECT c.id AS customer_id, c.company, o.amount
  FROM customers c
  JOIN orders o ON c.id = o.customer_id;

-- A partitioned table reports TABLE_TYPE='BASE TABLE' in TiDB, so it is
-- ingested as a regular "Table" subtype (partitioning is transparent here).
CREATE TABLE events (
  id      INT  NOT NULL,
  ts      DATE NOT NULL,
  PRIMARY KEY (id, ts)
) PARTITION BY RANGE (YEAR(ts)) (
  PARTITION p0 VALUES LESS THAN (2020),
  PARTITION p1 VALUES LESS THAN (2030)
);

-- Sequences are a TiDB-native object type with no MySQL equivalent. The
-- SQLAlchemy MySQL dialect returns them from neither get_table_names() nor
-- get_view_names(), so the source skips them entirely. This sequence is in the
-- fixture to document/lock that behavior: it must NOT appear in the golden.
CREATE SEQUENCE order_id_seq;

INSERT INTO customers (id, company, email_address) VALUES
  (1, 'Company A', 'a@example.com'),
  (2, 'Company B', 'b@example.com');

INSERT INTO orders (id, customer_id, amount) VALUES
  (1, 1, 100.50),
  (2, 2, 250.00);
