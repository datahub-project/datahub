CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    value NUMERIC(10,2)
);

CREATE INDEX idx_test_table_name ON test_table(name);

CREATE VIEW test_view AS
SELECT id, name, value
FROM test_table
WHERE value > 0;

INSERT INTO test_table (name, description, value) VALUES
('Test 1', 'Description 1', 100.50),
('Test 2', 'Description 2', 200.75),
('Test 3', 'Description 3', 300.25);

CREATE TABLE referenced_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE referencing_table (
    id SERIAL PRIMARY KEY,
    ref_id INTEGER REFERENCES referenced_table(id),
    name VARCHAR(100)
);
