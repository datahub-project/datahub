CREATE DATABASE IF NOT EXISTS test_usage_db;
USE test_usage_db;

CREATE TABLE raw_customer_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(255)
);

CREATE TABLE processed_customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    full_name VARCHAR(100),
    email_domain VARCHAR(100)
);

INSERT INTO raw_customer_data (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@test.com'),
    ('Bob Wilson', 'bob@domain.com');
