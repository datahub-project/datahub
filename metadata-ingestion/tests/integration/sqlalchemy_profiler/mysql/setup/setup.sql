-- Test data for the MySQL SQLAlchemy profiler integration tests.
-- All tables have known values for exact validation. Mirrors the Postgres
-- sqlalchemy_profiler suite with MySQL-adapted types (BOOLEAN -> TINYINT(1),
-- TIMESTAMP -> DATETIME). test_distinct_heavy is new: it carries enough
-- COUNT(DISTINCT) aggregates (7 columns) to exercise the
-- MAX_DISTINCT_PER_STATEMENT cap splitting in the flatten path.

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

-- Values: [1, 2, 3, 4, 5, NULL, NULL]
-- Expected: row_count=7, non_null=5, null=2, min=1, max=5, mean=3.0, stdev~1.5811
CREATE TABLE test_exact_numeric (
    id INT PRIMARY KEY,
    value_col INT
);

INSERT INTO test_exact_numeric (id, value_col) VALUES
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, NULL), (7, NULL);

-- Table with various data types
CREATE TABLE test_mixed_types (
    id INT PRIMARY KEY,
    int_col INT,
    float_col FLOAT,
    decimal_col DECIMAL(10,2),
    varchar_col VARCHAR(100),
    text_col TEXT,
    date_col DATE,
    timestamp_col DATETIME,
    bool_col TINYINT(1)
);

INSERT INTO test_mixed_types (
    id, int_col, float_col, decimal_col, varchar_col, text_col,
    date_col, timestamp_col, bool_col
) VALUES
    (1, 100, 10.5, 99.99, 'test1', 'text1', '2024-01-01', '2024-01-01 10:00:00', 1),
    (2, 200, 20.5, 199.99, 'test2', 'text2', '2024-01-02', '2024-01-02 11:00:00', 0),
    (3, 300, 30.5, 299.99, 'test3', 'text3', '2024-01-03', '2024-01-03 12:00:00', 1),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

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

-- Distinct-heavy table for the flatten off/on decisive test.
-- 8 columns (id + col_a..col_g), so 8 COUNT(DISTINCT) aggregates; with
-- include_field_distinct_count enabled and MAX_DISTINCT_PER_STATEMENT=5 that
-- splits into 2 distinct-chunk flat statements, so the off/on comparison
-- exercises the cap splitting path, not just cheap aggregates.
CREATE TABLE test_distinct_heavy (
    id INT PRIMARY KEY,
    col_a INT,
    col_b INT,
    col_c INT,
    col_d INT,
    col_e INT,
    col_f INT,
    col_g INT
);

INSERT INTO test_distinct_heavy (id, col_a, col_b, col_c, col_d, col_e, col_f, col_g) VALUES
    (1,  1,  2,  3,  4,  5,  6,  7),
    (2,  2,  3,  4,  5,  6,  7,  8),
    (3,  3,  4,  5,  6,  7,  8,  9),
    (4,  4,  5,  6,  7,  8,  9, 10),
    (5,  5,  6,  7,  8,  9, 10, 11),
    (6,  6,  7,  8,  9, 10, 11, 12),
    (7,  7,  8,  9, 10, 11, 12, 13),
    (8,  8,  9, 10, 11, 12, 13, 14),
    (9,  9, 10, 11, 12, 13, 14, 15),
    (10, 10, 11, 12, 13, 14, 15, 16);
