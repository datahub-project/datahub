-- Initialization script for the test PostgreSQL database

-- Create test schemas
CREATE SCHEMA IF NOT EXISTS source_schema;
CREATE SCHEMA IF NOT EXISTS destination_schema;

-- Create test tables in source_schema
CREATE TABLE IF NOT EXISTS source_schema.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_schema.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES source_schema.customers(customer_id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data into customers
INSERT INTO source_schema.customers (name, email, country)
VALUES
    ('John Doe', 'john.doe@example.com', 'USA'),
    ('Jane Smith', 'jane.smith@example.com', 'Canada'),
    ('Alice Johnson', 'alice.johnson@example.com', 'UK'),
    ('Bob Brown', 'bob.brown@example.com', 'Australia');

-- Insert sample data into orders
INSERT INTO source_schema.orders (customer_id, product_name, quantity, price)
VALUES
    (1, 'Laptop', 1, 1200.00),
    (1, 'Mouse', 1, 25.50),
    (2, 'Monitor', 2, 350.00),
    (3, 'Keyboard', 1, 85.75),
    (4, 'Headphones', 1, 120.00);

-- Grant privileges to test user
GRANT ALL PRIVILEGES ON SCHEMA source_schema TO test;
GRANT ALL PRIVILEGES ON SCHEMA destination_schema TO test;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA source_schema TO test;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA destination_schema TO test;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA source_schema TO test;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA destination_schema TO test;