-- Test data for custom SQL profiler integration tests
-- All tables have known values for exact validation

-- Table with known exact values for basic statistics
-- Values: [1, 2, 3, 4, 5, NULL, NULL]
-- Expected: row_count=7, non_null=5, null=2, min=1, max=5, mean=3.0, stdev≈1.5811
CREATE TABLE test_exact_numeric (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_exact_numeric (id, value_col) VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5),
    (6, NULL),
    (7, NULL);

-- Table with known values for quantile testing
-- Values: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
-- Expected quantiles: q_0.5=55, q_0.25=30, q_0.75=80
CREATE TABLE test_quantiles (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_quantiles (id, value_col) VALUES
    (1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
    (6, 60), (7, 70), (8, 80), (9, 90), (10, 100);

-- Table with known value frequencies
-- Values: ['A', 'A', 'A', 'B', 'B', 'C']
-- Expected: A=3, B=2, C=1
CREATE TABLE test_frequencies (
    id INT PRIMARY KEY,
    category_col VARCHAR(10)
);

INSERT INTO test_frequencies (id, category_col) VALUES
    (1, 'A'),
    (2, 'A'),
    (3, 'A'),
    (4, 'B'),
    (5, 'B'),
    (6, 'C');

-- Table with various data types
CREATE TABLE test_mixed_types (
    id INT PRIMARY KEY,
    int_col INT,
    float_col FLOAT,
    decimal_col DECIMAL(10,2),
    varchar_col VARCHAR(100),
    text_col TEXT,
    date_col DATE,
    timestamp_col TIMESTAMP,
    bool_col BOOLEAN
);

INSERT INTO test_mixed_types (
    id, int_col, float_col, decimal_col, varchar_col, text_col,
    date_col, timestamp_col, bool_col
) VALUES
    (1, 100, 10.5, 99.99, 'test1', 'text1', '2024-01-01', '2024-01-01 10:00:00', TRUE),
    (2, 200, 20.5, 199.99, 'test2', 'text2', '2024-01-02', '2024-01-02 11:00:00', FALSE),
    (3, 300, 30.5, 299.99, 'test3', 'text3', '2024-01-03', '2024-01-03 12:00:00', TRUE),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Table for testing unique counts (exact and approximate)
-- 10 distinct values, each appearing once
CREATE TABLE test_unique_count (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_unique_count (id, value_col) VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);

-- Table for testing histogram
-- Values distributed across range [0, 100]
CREATE TABLE test_histogram (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_histogram (id, value_col) VALUES
    (1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
    (6, 60), (7, 70), (8, 80), (9, 90), (10, 100);

-- Edge case: Empty table
CREATE TABLE test_empty (
    id INT PRIMARY KEY,
    value_col INT
);

-- Edge case: Single row
CREATE TABLE test_single_row (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_single_row (id, value_col) VALUES (1, 42);

-- Edge case: All NULLs
CREATE TABLE test_all_nulls (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_all_nulls (id, value_col) VALUES
    (1, NULL), (2, NULL), (3, NULL);

-- Table for testing row count estimation
-- Will be used with profile_table_row_count_estimate_only
CREATE TABLE test_row_count_estimation (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_row_count_estimation (id, value_col)
SELECT generate_series(1, 1000), (random() * 100)::INT;

-- Update statistics for row count estimation (pg_class.reltuples)
ANALYZE test_row_count_estimation;

-- Table for testing cardinality filtering for quantiles
-- Low cardinality (TWO) - quantiles should NOT be calculated
CREATE TABLE test_low_cardinality (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_low_cardinality (id, value_col) VALUES
    (1, 10), (2, 10), (3, 10), (4, 10), (5, 10),
    (6, 20), (7, 20), (8, 20), (9, 20), (10, 20);

-- Table for testing cardinality filtering for quantiles
-- High cardinality (FEW) - quantiles SHOULD be calculated
-- 25 unique values out of 50 rows -> pct_unique = 0.5 -> FEW cardinality
CREATE TABLE test_high_cardinality (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_high_cardinality (id, value_col)
SELECT i, (i % 25) + 1
FROM generate_series(1, 50) AS i;

