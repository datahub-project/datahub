CREATE DATABASE IF NOT EXISTS test_db;
USE test_db;

-- Raw data tables (input tables)
CREATE TABLE raw_customer_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_order_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_total DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES raw_customer_data(id)
);

-- Processed/Output tables
CREATE TABLE processed_customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    full_name VARCHAR(100),
    email_domain VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES raw_customer_data(id)
);

CREATE TABLE customer_order_summary (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    total_orders INT,
    total_spent DECIMAL(10, 2),
    last_order_date TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES raw_customer_data(id)
);

-- Insert sample data
INSERT INTO raw_customer_data (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@test.com'),
    ('Bob Wilson', 'bob@domain.com');

INSERT INTO raw_order_data (customer_id, order_total) VALUES
    (1, 100.50),
    (1, 200.75),
    (2, 150.25),
    (3, 300.00),
    (2, 175.50);

-- Create stored procedures with clear lineage
DELIMITER //

-- Procedure 1: Process customer data
CREATE PROCEDURE process_customer_data()
BEGIN
    -- Clear existing processed data
    DELETE FROM processed_customers;

    -- Insert processed customer data
    INSERT INTO processed_customers (customer_id, full_name, email_domain)
    SELECT
        id as customer_id,
        name as full_name,
        SUBSTRING_INDEX(email, '@', -1) as email_domain
    FROM raw_customer_data;
END //

-- Procedure 2: Generate customer order summaries
CREATE PROCEDURE generate_order_summaries(IN min_order_total DECIMAL(10,2))
BEGIN
    -- Clear existing summaries
    DELETE FROM customer_order_summary;

    -- Create order summaries
    INSERT INTO customer_order_summary (
        customer_id,
        total_orders,
        total_spent,
        last_order_date
    )
    SELECT
        rc.id as customer_id,
        COUNT(ro.id) as total_orders,
        SUM(ro.order_total) as total_spent,
        MAX(ro.created_at) as last_order_date
    FROM raw_customer_data rc
    LEFT JOIN raw_order_data ro ON rc.id = ro.customer_id
    GROUP BY rc.id
    HAVING SUM(ro.order_total) >= min_order_total;
END //

-- Procedure 3: Refresh all customer data
CREATE PROCEDURE refresh_customer_analytics()
BEGIN
    -- Process base customer data first
    CALL process_customer_data();

    -- Then generate order summaries for customers with orders over $100
    CALL generate_order_summaries(100.00);
END //

DELIMITER ;
