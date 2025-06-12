CREATE DATABASE IF NOT EXISTS test;
USE test;

-- Create tables
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB;

-- Create indexes
CREATE INDEX idx_user_email ON users(email);
CREATE INDEX idx_order_user ON orders(user_id);

-- Create views
CREATE VIEW active_users_view AS
SELECT u.id, u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name, u.email;

CREATE VIEW large_orders_view AS
SELECT o.id as order_id, o.amount, u.name as user_name, u.email
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.amount > 1000;

-- Create stored procedure
DELIMITER //
CREATE PROCEDURE update_order_status(
    IN order_id_param INT,
    IN new_status VARCHAR(50)
)
BEGIN
    UPDATE orders
    SET status = new_status
    WHERE id = order_id_param;
END //
DELIMITER ;

-- Insert test data
INSERT INTO users (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com'),
    ('Bob Wilson', 'bob@example.com');

INSERT INTO orders (user_id, amount, status) VALUES
    (1, 1500.00, 'COMPLETED'),
    (1, 750.50, 'PENDING'),
    (2, 2000.00, 'COMPLETED'),
    (3, 500.00, 'PENDING');