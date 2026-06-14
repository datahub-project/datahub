-- Setup script for test PostgreSQL database
-- This creates sample tables that will be used in Metabase queries

-- Create a sample products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a sample orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    order_date DATE,
    customer_name VARCHAR(255)
);

-- Create a sample customers table
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data into products
INSERT INTO products (name, category, price) VALUES
    ('Widget A', 'Electronics', 29.99),
    ('Widget B', 'Electronics', 49.99),
    ('Gadget X', 'Accessories', 19.99),
    ('Gadget Y', 'Accessories', 39.99),
    ('Tool Z', 'Tools', 99.99);

-- Insert sample data into customers
INSERT INTO customers (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com'),
    ('Bob Johnson', 'bob@example.com');

-- Insert sample data into orders
INSERT INTO orders (product_id, quantity, order_date, customer_name) VALUES
    (1, 2, '2024-01-15', 'John Doe'),
    (2, 1, '2024-01-16', 'Jane Smith'),
    (3, 5, '2024-01-17', 'Bob Johnson'),
    (1, 3, '2024-01-18', 'Jane Smith'),
    (4, 2, '2024-01-19', 'John Doe');
