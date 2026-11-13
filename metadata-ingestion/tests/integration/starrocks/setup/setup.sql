-- Create test database
CREATE DATABASE IF NOT EXISTS test_db;

-- Create tables in test_db
USE test_db;

CREATE TABLE IF NOT EXISTS customers (
    id INT NOT NULL,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at DATETIME
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS orders (
    id INT NOT NULL,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10, 2)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS products (
    id INT NOT NULL,
    name VARCHAR(200),
    price DECIMAL(10, 2),
    category VARCHAR(50)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- Insert sample data
INSERT INTO customers VALUES
    (1, 'Alice Smith', 'alice@example.com', '2024-01-01 10:00:00'),
    (2, 'Bob Johnson', 'bob@example.com', '2024-01-02 11:00:00'),
    (3, 'Carol White', 'carol@example.com', '2024-01-03 12:00:00');

INSERT INTO orders VALUES
    (1, 1, '2024-01-15', 150.00),
    (2, 1, '2024-01-20', 200.50),
    (3, 2, '2024-01-22', 75.25);

INSERT INTO products VALUES
    (1, 'Widget A', 29.99, 'Electronics'),
    (2, 'Widget B', 49.99, 'Electronics'),
    (3, 'Gadget C', 99.99, 'Gadgets');
