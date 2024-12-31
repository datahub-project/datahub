CREATE DATABASE test;
GO
USE test;
GO

-- Create tables
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    created_at DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE orders (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_orders_users FOREIGN KEY (user_id) REFERENCES users(id)
);
GO

-- Create indexes
CREATE INDEX idx_user_email ON users(email);
CREATE INDEX idx_order_user ON orders(user_id);
GO

-- Create views
CREATE VIEW active_users_view AS
SELECT u.id, u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name, u.email;
GO

CREATE VIEW large_orders_view AS
SELECT o.id as order_id, o.amount, u.name as user_name, u.email
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.amount > 1000;
GO

-- Create stored procedure
CREATE PROCEDURE update_order_status
    @order_id_param INT,
    @new_status VARCHAR(50)
AS
BEGIN
    UPDATE orders
    SET status = @new_status
    WHERE id = @order_id_param;
END;
GO

-- Insert test data
INSERT INTO users (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com'),
    ('Bob Wilson', 'bob@example.com');
GO

INSERT INTO orders (user_id, amount, status) VALUES
    (1, 1500.00, 'COMPLETED'),
    (1, 750.50, 'PENDING'),
    (2, 2000.00, 'COMPLETED'),
    (3, 500.00, 'PENDING');
GO