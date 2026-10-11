-- PostgreSQL test data for recording/replay integration tests
-- Creates multiple schemas, tables, views with relationships to generate
-- a good number of SQL queries during ingestion

-- Create schemas
CREATE SCHEMA sales;
CREATE SCHEMA hr;
CREATE SCHEMA analytics;

-- Sales schema tables
CREATE TABLE sales.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sales.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sales.orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES sales.customers(id),
    product_id INT REFERENCES sales.products(id),
    quantity INT NOT NULL,
    total_amount DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT NOW()
);

-- HR schema tables
CREATE TABLE hr.departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    budget DECIMAL(12,2)
);

CREATE TABLE hr.employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INT REFERENCES hr.departments(id),
    salary DECIMAL(10,2),
    hire_date DATE DEFAULT CURRENT_DATE
);

-- Analytics schema (views for lineage testing)
CREATE VIEW analytics.customer_summary AS
SELECT 
    c.id,
    c.name,
    c.email,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent
FROM sales.customers c
LEFT JOIN sales.orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email;

CREATE VIEW analytics.product_sales AS
SELECT 
    p.id,
    p.name,
    p.category,
    COUNT(o.id) as units_sold,
    COALESCE(SUM(o.total_amount), 0) as total_revenue
FROM sales.products p
LEFT JOIN sales.orders o ON p.id = o.product_id
GROUP BY p.id, p.name, p.category;

CREATE VIEW analytics.department_headcount AS
SELECT 
    d.name as department,
    COUNT(e.id) as employee_count,
    AVG(e.salary) as avg_salary
FROM hr.departments d
LEFT JOIN hr.employees e ON d.id = e.department_id
GROUP BY d.id, d.name;

-- Insert test data

-- Departments (5 departments)
INSERT INTO hr.departments (name, budget) VALUES
    ('Engineering', 1000000),
    ('Sales', 500000),
    ('Marketing', 300000),
    ('HR', 200000),
    ('Operations', 400000);

-- Employees (50 employees)
INSERT INTO hr.employees (name, department_id, salary, hire_date)
SELECT 
    'Employee ' || i,
    ((i - 1) % 5) + 1,  -- Distribute across 5 departments
    45000 + (i * 1500),
    CURRENT_DATE - (i * 10 || ' days')::INTERVAL
FROM generate_series(1, 50) i;

-- Customers (100 customers)
INSERT INTO sales.customers (name, email, created_at)
SELECT 
    'Customer ' || i,
    'customer' || i || '@example.com',
    NOW() - (i * 7 || ' days')::INTERVAL
FROM generate_series(1, 100) i;

-- Products (30 products)
INSERT INTO sales.products (name, price, category, created_at)
SELECT 
    'Product ' || i,
    9.99 + (i * 5.00),
    CASE 
        WHEN i % 4 = 0 THEN 'Electronics'
        WHEN i % 4 = 1 THEN 'Clothing'
        WHEN i % 4 = 2 THEN 'Food'
        ELSE 'Home & Garden'
    END,
    NOW() - (i * 3 || ' days')::INTERVAL
FROM generate_series(1, 30) i;

-- Orders (200 orders - creates good join activity)
INSERT INTO sales.orders (customer_id, product_id, quantity, total_amount, order_date)
SELECT 
    ((i - 1) % 100) + 1,  -- Random customer
    ((i - 1) % 30) + 1,   -- Random product
    (i % 5) + 1,          -- Quantity 1-5
    ((i % 5) + 1) * (9.99 + (((i - 1) % 30) + 1) * 5.00),  -- Total = quantity * price
    NOW() - (i || ' hours')::INTERVAL
FROM generate_series(1, 200) i;

-- Add indexes (metadata ingestion will discover these)
CREATE INDEX idx_orders_customer ON sales.orders(customer_id);
CREATE INDEX idx_orders_product ON sales.orders(product_id);
CREATE INDEX idx_employees_dept ON hr.employees(department_id);
CREATE INDEX idx_customers_email ON sales.customers(email);

-- Add comments (metadata)
COMMENT ON SCHEMA sales IS 'Sales and customer data';
COMMENT ON SCHEMA hr IS 'Human resources data';
COMMENT ON SCHEMA analytics IS 'Analytics views and aggregations';

COMMENT ON TABLE sales.customers IS 'Customer master data';
COMMENT ON TABLE sales.orders IS 'Order transactions';
COMMENT ON TABLE sales.products IS 'Product catalog';
COMMENT ON TABLE hr.employees IS 'Employee information';
COMMENT ON TABLE hr.departments IS 'Department structure';

COMMENT ON COLUMN sales.customers.email IS 'Customer email address (unique)';
COMMENT ON COLUMN sales.orders.total_amount IS 'Total order amount in USD';
COMMENT ON COLUMN hr.employees.salary IS 'Annual salary in USD';

