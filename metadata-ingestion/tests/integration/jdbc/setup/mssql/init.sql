CREATE TABLE test_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100),
    description NVARCHAR(MAX),
    created_at DATETIME DEFAULT GETDATE(),
    value DECIMAL(10,2)
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
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100)
);

CREATE TABLE referencing_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ref_id INT,
    name NVARCHAR(100),
    CONSTRAINT FK_referencing_referenced FOREIGN KEY (ref_id)
        REFERENCES referenced_table(id)
);
