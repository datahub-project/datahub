CREATE DATABASE testdb WITH LOG;

CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(255)
);

CREATE TABLE orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER REFERENCES customers(id),
  amount DECIMAL(10,2)
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
