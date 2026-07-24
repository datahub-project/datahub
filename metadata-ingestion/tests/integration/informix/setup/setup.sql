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
