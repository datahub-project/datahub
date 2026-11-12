CREATE DATABASE testdb WITH LOG;

CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(255),
  -- Declared with reserved minimum space, so syscolumns.collength packs it as
  -- (10 * 256) + 60 = 2620. Guards the collength decode in mapping.py.
  notes VARCHAR(60,10),
  -- A reserved minimum of 128 or more overflows collength's signed SMALLINT:
  -- (150 * 256) + 200 = 38600 is stored as -26936. Guards the sign-flip path
  -- against the real driver, not just a literal in the unit tests.
  bio VARCHAR(200,150)
);

CREATE TABLE orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER REFERENCES customers(id),
  amount DECIMAL(10,2)
);

-- Extended types: LVARCHAR is coltype 40 and BOOLEAN/BLOB/CLOB all share
-- coltype 41, so none of them can be identified without the sysxtdtypes join.
-- A DISTINCT over an ordinary built-in keeps it in the low byte of coltype
-- (money_usd is 2053 = 2048 | 5, DECIMAL), but a DISTINCT over an opaque
-- built-in cannot: flag_type is 18473 and doc_text is 2089, both low byte 41,
-- so only sysxtdtypes.source names what they were declared over.
CREATE DISTINCT TYPE money_usd AS DECIMAL(12,2);
CREATE DISTINCT TYPE flag_type AS BOOLEAN;
CREATE DISTINCT TYPE doc_text AS CLOB;

CREATE ROW TYPE address_t (street VARCHAR(50), city VARCHAR(30));

CREATE TABLE documents (
  doc_id INTEGER PRIMARY KEY,
  body LVARCHAR(4000),
  archived BOOLEAN,
  payload BLOB,
  summary CLOB,
  price money_usd,
  published flag_type,
  abstract doc_text,
  mailing_address address_t
);

CREATE VIEW active_customers AS SELECT id, name FROM customers;

CREATE VIEW customer_orders AS
  SELECT c.id AS customer_id, c.name AS customer_name, o.order_id, o.amount
  FROM customers c, orders o WHERE c.id = o.customer_id;

INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO customers (id, name, email) VALUES (2, 'Bob', 'bob@example.com');
INSERT INTO orders (order_id, customer_id, amount) VALUES (1, 1, 10.50);
INSERT INTO orders (order_id, customer_id, amount) VALUES (2, 2, 20.00);

UPDATE STATISTICS FOR TABLE customers;
UPDATE STATISTICS FOR TABLE orders;
