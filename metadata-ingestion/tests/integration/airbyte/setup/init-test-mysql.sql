-- Initialization script for the test MySQL database

-- Create additional test databases
CREATE DATABASE IF NOT EXISTS source_db;
CREATE DATABASE IF NOT EXISTS destination_db;

-- Grant privileges to test user
GRANT ALL PRIVILEGES ON source_db.* TO 'test'@'%';
GRANT ALL PRIVILEGES ON destination_db.* TO 'test'@'%';
GRANT ALL PRIVILEGES ON test.* TO 'test'@'%';

-- Use the source database
USE source_db;

-- Create test tables
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Insert sample data into customers
INSERT INTO customers (name, email, country)
VALUES
    ('John Doe', 'john.doe@example.com', 'USA'),
    ('Jane Smith', 'jane.smith@example.com', 'Canada'),
    ('Alice Johnson', 'alice.johnson@example.com', 'UK'),
    ('Bob Brown', 'bob.brown@example.com', 'Australia');

-- Insert sample data into orders
INSERT INTO orders (customer_id, product_name, quantity, price)
VALUES
    (1, 'Laptop', 1, 1200.00),
    (1, 'Mouse', 1, 25.50),
    (2, 'Monitor', 2, 350.00),
    (3, 'Keyboard', 1, 85.75),
    (4, 'Headphones', 1, 120.00);

-- Flush privileges to ensure changes take effect
FLUSH PRIVILEGES;