-- Create test database and sample data for StarRocks integration tests
create database test_db;
use test_db;

-- Sample employees table for testing StarRocks analytics features
CREATE TABLE employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      VARCHAR(1)      NOT NULL,    
    hire_date   DATE            NOT NULL
)
DUPLICATE KEY (emp_no)
DISTRIBUTED BY HASH(emp_no) BUCKETS 3;

CREATE TABLE salaries (
    emp_no      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    salary      INT             NOT NULL,
    to_date     DATE            NOT NULL
)
DUPLICATE KEY (emp_no, from_date)
DISTRIBUTED BY HASH(emp_no) BUCKETS 3;

-- Sample data
INSERT INTO employees VALUES (10001,'1953-09-02','Georgi','Facello','M','1986-06-26'),
(10002,'1964-06-02','Bezalel','Simmel','F','1985-11-21'),
(10003,'1959-12-03','Parto','Bamford','M','1986-08-28'),
(10004,'1954-05-01','Chirstian','Koblick','M','1986-12-01'),
(10005,'1955-01-21','Kyoichi','Maliniak','M','1989-09-12');

INSERT INTO salaries VALUES 
(10001,'1986-06-26',60117,'1987-06-26'),
(10002,'1996-08-03',65828,'1997-08-03'),
(10003,'1995-12-03',40006,'1996-12-02'),
(10004,'1986-12-01',40054,'1987-12-01'),
(10005,'1989-09-12',78228,'1990-09-11');

-- Create a StarRocks-specific aggregate table for testing OLAP features
CREATE TABLE sales_summary (
    sale_date DATE,
    region VARCHAR(50),
    product_category VARCHAR(50),
    total_sales DECIMAL(15,2),
    total_quantity INT
) 
DUPLICATE KEY (sale_date, region, product_category)
DISTRIBUTED BY HASH(region) BUCKETS 3;

INSERT INTO sales_summary VALUES
('2023-01-01', 'North', 'Electronics', 15000.50, 150),
('2023-01-01', 'South', 'Electronics', 12000.75, 120),
('2023-01-01', 'East', 'Clothing', 8000.25, 200),
('2023-01-02', 'North', 'Electronics', 18000.00, 180),
('2023-01-02', 'West', 'Books', 3500.50, 70);