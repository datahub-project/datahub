-- Create tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
CREATE OR REPLACE PROCEDURE update_order_status(
    order_id_param INTEGER,
    new_status VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE orders
    SET status = new_status
    WHERE id = order_id_param;
END;
$$;

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
