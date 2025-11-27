-- Setup script for Apache Doris integration tests
-- Note: Doris syntax differs from MySQL in some ways

DROP DATABASE IF EXISTS dorisdb;
CREATE DATABASE IF NOT EXISTS dorisdb;
USE dorisdb;

-- Table: customers
-- Doris doesn't support INT(11) syntax, just use INT
CREATE TABLE IF NOT EXISTS customers (
  customer_id INT NOT NULL,
  customer_name VARCHAR(100) NOT NULL,
  email VARCHAR(100),
  country VARCHAR(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

-- Table: orders
CREATE TABLE IF NOT EXISTS orders (
  order_id INT NOT NULL,
  customer_id INT NOT NULL,
  order_date DATE,
  total_amount DECIMAL(10,2),
  status VARCHAR(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

-- Table: analytics_data
-- In real Doris, this would use HLL, BITMAP, ARRAY, JSONB types
-- For now, using compatible types with comments
CREATE TABLE IF NOT EXISTS analytics_data (
  id INT NOT NULL,
  name VARCHAR(100),
  -- user_ids_hll would be HLL type in production
  user_ids_hll STRING COMMENT 'HyperLogLog type in production Doris',
  -- user_bitmap would be BITMAP type in production
  user_bitmap STRING COMMENT 'Bitmap type in production Doris',
  -- tags would be ARRAY<VARCHAR(50)> in production
  tags STRING COMMENT 'Array type in production Doris',
  -- metadata would be JSONB in production
  metadata STRING COMMENT 'JSONB type in production Doris',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

-- Create a view
CREATE VIEW IF NOT EXISTS customer_orders AS
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;

-- Insert sample data
INSERT INTO customers (customer_id, customer_name, email, country) VALUES
(1, 'Alice Smith', 'alice@example.com', 'USA'),
(2, 'Bob Johnson', 'bob@example.com', 'UK'),
(3, 'Charlie Brown', 'charlie@example.com', 'Canada');

INSERT INTO orders (order_id, customer_id, order_date, total_amount, status) VALUES
(1001, 1, '2024-01-15', 150.00, 'completed'),
(1002, 1, '2024-02-20', 200.00, 'completed'),
(1003, 2, '2024-01-25', 75.50, 'completed'),
(1004, 3, '2024-03-01', 300.00, 'pending');

INSERT INTO analytics_data (id, name, tags, metadata) VALUES
(1, 'Test Record', 'tag1,tag2', '{"key":"value"}');
