-- Setup script for Apache Doris integration tests
-- Note: Doris syntax differs from MySQL in some ways

-- Database 1: Main test database
DROP DATABASE IF EXISTS dorisdb;
CREATE DATABASE IF NOT EXISTS dorisdb;

-- Database 2: Secondary database for multi-database testing
DROP DATABASE IF EXISTS dorisdb_analytics;
CREATE DATABASE IF NOT EXISTS dorisdb_analytics;

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
-- Uses Doris-specific types: HLL, BITMAP, ARRAY, JSONB, QUANTILE_STATE
-- Note: Aggregate table with special column types
-- IMPORTANT: Key columns (id, name, created_at) MUST be first in schema
-- IMPORTANT: ALL non-key columns MUST specify aggregation type
CREATE TABLE IF NOT EXISTS analytics_data (
  id INT NOT NULL,
  name VARCHAR(100),
  created_at DATETIME,
  user_ids_hll HLL HLL_UNION COMMENT 'HyperLogLog for cardinality estimation',
  user_bitmap BITMAP BITMAP_UNION COMMENT 'Bitmap for set operations',
  tags ARRAY<VARCHAR(50)> REPLACE COMMENT 'Array of tags',
  metadata JSONB REPLACE COMMENT 'JSON binary format',
  percentile_data QUANTILE_STATE QUANTILE_UNION COMMENT 'Quantile state for percentile calculations'
)
AGGREGATE KEY(id, name, created_at)
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

-- Insert sample data for analytics_data
-- Note: HLL, BITMAP, and QUANTILE_STATE require aggregation on insert
-- Using SELECT with constructors for these special types
-- Column order MUST match table schema: id, name, created_at, user_ids_hll, user_bitmap, tags, metadata, percentile_data
INSERT INTO analytics_data (id, name, created_at, user_ids_hll, user_bitmap, tags, metadata, percentile_data)
SELECT 
  1 as id,
  'Test Record' as name,
  NOW() as created_at,
  hll_hash(1) as user_ids_hll,
  to_bitmap(1) as user_bitmap,
  ['tag1', 'tag2'] as tags,
  CAST('{"key":"value"}' AS JSONB) as metadata,
  to_quantile_state(0.5, 2048) as percentile_data;

-- Create tables in second database for multi-database testing
USE dorisdb_analytics;

CREATE TABLE IF NOT EXISTS metrics (
  metric_id INT NOT NULL,
  metric_name VARCHAR(100) NOT NULL,
  metric_value DOUBLE,
  event_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(metric_id)
DISTRIBUTED BY HASH(metric_id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS user_segments (
  segment_id INT NOT NULL,
  segment_name VARCHAR(100),
  created_at DATETIME,
  user_ids BITMAP BITMAP_UNION COMMENT 'Bitmap of user IDs in this segment'
)
AGGREGATE KEY(segment_id, segment_name, created_at)
DISTRIBUTED BY HASH(segment_id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

INSERT INTO metrics (metric_id, metric_name, metric_value) VALUES
(1, 'daily_active_users', 1250.5),
(2, 'conversion_rate', 0.045);

INSERT INTO user_segments (segment_id, segment_name, created_at, user_ids)
SELECT 1 as segment_id, 'power_users' as segment_name, NOW() as created_at, to_bitmap(100) as user_ids
UNION ALL
SELECT 2, 'new_users', NOW(), to_bitmap(200);
