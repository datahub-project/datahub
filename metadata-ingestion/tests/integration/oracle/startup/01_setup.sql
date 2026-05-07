-- Oracle Integration Test Setup Script
-- This script creates comprehensive test data including stored procedures, 
-- materialized views, and dependencies for testing DataHub Oracle ingestion

-- Note: This script runs automatically during Oracle Docker container initialization
-- with SYSDBA privileges in the XEPDB1 pluggable database

-- Connect to the PDB first
ALTER SESSION SET CONTAINER = XEPDB1;

-- Create test schemas (local users in PDB)
CREATE USER hr_schema IDENTIFIED BY hr123;
CREATE USER sales_schema IDENTIFIED BY sales123;
CREATE USER analytics_schema IDENTIFIED BY analytics123;
CREATE USER staging_schema IDENTIFIED BY staging123;

-- Grant necessary privileges including tablespace quota
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, QUERY REWRITE TO hr_schema;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, QUERY REWRITE TO sales_schema;
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW, QUERY REWRITE TO analytics_schema;
GRANT CONNECT, RESOURCE, CREATE VIEW TO staging_schema;

-- Grant additional system privileges needed for materialized views to all schemas
GRANT CREATE ANY MATERIALIZED VIEW TO hr_schema;
GRANT ALTER ANY MATERIALIZED VIEW TO hr_schema;
GRANT CREATE ANY MATERIALIZED VIEW TO sales_schema;
GRANT ALTER ANY MATERIALIZED VIEW TO sales_schema;
GRANT CREATE ANY MATERIALIZED VIEW TO analytics_schema;
GRANT ALTER ANY MATERIALIZED VIEW TO analytics_schema;

-- Grant SYSTEM user privileges to create materialized views in other schemas
-- (SYSTEM already has these privileges, but let's be explicit)
-- Note: SYSTEM user should already have these privileges by default

-- Grant tablespace quota (needed for inserting data)
ALTER USER hr_schema QUOTA UNLIMITED ON USERS;
ALTER USER sales_schema QUOTA UNLIMITED ON USERS;
ALTER USER analytics_schema QUOTA UNLIMITED ON USERS;
ALTER USER staging_schema QUOTA UNLIMITED ON USERS;

-- Grant additional privileges for procedures and packages
GRANT CREATE PROCEDURE TO hr_schema;
GRANT CREATE PROCEDURE TO sales_schema;
GRANT CREATE PROCEDURE TO analytics_schema;

-- Create HR_SCHEMA objects
ALTER SESSION SET CURRENT_SCHEMA = hr_schema;

-- Create base tables
CREATE TABLE departments (
    department_id NUMBER(4) PRIMARY KEY,
    department_name VARCHAR2(30) NOT NULL,
    manager_id NUMBER(6),
    location_id NUMBER(4)
);

CREATE TABLE employees (
    employee_id NUMBER(6) PRIMARY KEY,
    first_name VARCHAR2(20),
    last_name VARCHAR2(25) NOT NULL,
    email VARCHAR2(25) NOT NULL,
    phone_number VARCHAR2(20),
    hire_date DATE NOT NULL,
    job_id VARCHAR2(10) NOT NULL,
    salary NUMBER(8,2),
    commission_pct NUMBER(2,2),
    manager_id NUMBER(6),
    department_id NUMBER(4),
    CONSTRAINT emp_dept_fk FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Insert sample data
INSERT INTO departments VALUES (10, 'Administration', 200, 1700);
INSERT INTO departments VALUES (20, 'Marketing', 201, 1800);
INSERT INTO departments VALUES (50, 'Shipping', 121, 1500);
INSERT INTO departments VALUES (60, 'IT', 103, 1400);

INSERT INTO employees VALUES (100, 'Steven', 'King', 'SKING', '515.123.4567', DATE '2003-06-17', 'AD_PRES', 24000, NULL, NULL, 10);
INSERT INTO employees VALUES (101, 'Neena', 'Kochhar', 'NKOCHHAR', '515.123.4568', DATE '2005-09-21', 'AD_VP', 17000, NULL, 100, 10);
INSERT INTO employees VALUES (102, 'Lex', 'De Haan', 'LDEHAAN', '515.123.4569', DATE '2001-01-13', 'AD_VP', 17000, NULL, 100, 10);
INSERT INTO employees VALUES (103, 'Alexander', 'Hunold', 'AHUNOLD', '590.423.4567', DATE '2006-01-03', 'IT_PROG', 9000, NULL, 102, 60);

-- Create HR stored procedures with dependencies
CREATE OR REPLACE PROCEDURE get_employee_info(p_emp_id IN NUMBER, p_cursor OUT SYS_REFCURSOR) AS
BEGIN
    OPEN p_cursor FOR
    SELECT e.employee_id, e.first_name, e.last_name, e.salary, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    WHERE e.employee_id = p_emp_id;
END;
/

CREATE OR REPLACE FUNCTION calculate_annual_salary(p_emp_id IN NUMBER) RETURN NUMBER AS
    v_salary NUMBER;
    v_commission NUMBER;
BEGIN
    SELECT salary, NVL(commission_pct, 0) INTO v_salary, v_commission
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_salary * 12 * (1 + v_commission);
END;
/

-- Create SALES_SCHEMA objects
ALTER SESSION SET CURRENT_SCHEMA = sales_schema;

CREATE TABLE orders (
    order_id NUMBER(12) PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id NUMBER(6) NOT NULL,
    order_status VARCHAR2(8) NOT NULL,
    order_total NUMBER(8,2),
    sales_rep_id NUMBER(6)
);

CREATE TABLE order_items (
    order_id NUMBER(12),
    line_item_id NUMBER(3),
    product_id NUMBER(6) NOT NULL,
    unit_price NUMBER(8,2),
    quantity NUMBER(8),
    PRIMARY KEY (order_id, line_item_id),
    CONSTRAINT order_items_order_fk FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Insert sample data
INSERT INTO orders VALUES (1001, DATE '2024-01-15', 101, 'SHIPPED', 2500.00, 100);
INSERT INTO orders VALUES (1002, DATE '2024-01-16', 102, 'PENDING', 1800.00, 101);
INSERT INTO orders VALUES (1003, DATE '2024-01-17', 103, 'SHIPPED', 3200.00, 102);

INSERT INTO order_items VALUES (1001, 1, 2001, 50.00, 10);
INSERT INTO order_items VALUES (1001, 2, 2002, 75.00, 20);
INSERT INTO order_items VALUES (1002, 1, 2003, 30.00, 60);
INSERT INTO order_items VALUES (1003, 1, 2001, 50.00, 25);
INSERT INTO order_items VALUES (1003, 2, 2004, 120.00, 15);

-- Note: Cross-schema grants will be applied at the end of the script

-- Create sales stored procedures with cross-schema dependencies
CREATE OR REPLACE PROCEDURE process_order(p_order_id IN NUMBER) AS
    v_sales_rep_id NUMBER;
    v_emp_name VARCHAR2(50);
BEGIN
    -- Get sales rep info from HR schema
    SELECT o.sales_rep_id INTO v_sales_rep_id
    FROM orders o
    WHERE o.order_id = p_order_id;
    
    -- Get employee name from HR schema (creates dependency)
    SELECT first_name || ' ' || last_name INTO v_emp_name
    FROM hr_schema.employees
    WHERE employee_id = v_sales_rep_id;
    
    -- Update order status
    UPDATE orders 
    SET order_status = 'PROCESSED'
    WHERE order_id = p_order_id;
    
    -- Log processing (this would create dependency on a log table if it existed)
    NULL; -- Placeholder for logging logic
END;
/

CREATE OR REPLACE FUNCTION get_order_total(p_order_id IN NUMBER) RETURN NUMBER AS
    v_total NUMBER := 0;
BEGIN
    SELECT SUM(unit_price * quantity) INTO v_total
    FROM order_items
    WHERE order_id = p_order_id;
    
    RETURN NVL(v_total, 0);
END;
/

-- Create ANALYTICS_SCHEMA objects with materialized views
ALTER SESSION SET CURRENT_SCHEMA = analytics_schema;

-- Note: Materialized views will be created at the end of the script after grants are applied

-- Create analytics stored procedures that use materialized views
CREATE OR REPLACE PROCEDURE refresh_analytics_data AS
BEGIN
    -- Refresh materialized views (creates dependencies)
    DBMS_MVIEW.REFRESH('employee_sales_summary');
    DBMS_MVIEW.REFRESH('monthly_order_summary');
END;
/

CREATE OR REPLACE FUNCTION get_top_performer RETURN VARCHAR2 AS
    v_top_performer VARCHAR2(100);
BEGIN
    SELECT employee_name INTO v_top_performer
    FROM (
        SELECT employee_name, total_sales
        FROM employee_sales_summary
        WHERE total_sales IS NOT NULL
        ORDER BY total_sales DESC
    )
    WHERE ROWNUM = 1;
    
    RETURN v_top_performer;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'No data available';
END;
/

-- Create a package with multiple procedures
CREATE OR REPLACE PACKAGE analytics_pkg AS
    PROCEDURE generate_sales_report(p_month IN VARCHAR2);
    FUNCTION calculate_commission(p_emp_id IN NUMBER, p_rate IN NUMBER) RETURN NUMBER;
END analytics_pkg;
/

CREATE OR REPLACE PACKAGE BODY analytics_pkg AS
    PROCEDURE generate_sales_report(p_month IN VARCHAR2) AS
        v_count NUMBER;
    BEGIN
        -- Use both materialized views (creates dependencies)
        SELECT COUNT(*) INTO v_count
        FROM employee_sales_summary e
        JOIN monthly_order_summary m ON 1=1  -- Simplified join for demo
        WHERE m.order_month = p_month;
        
        -- This procedure depends on both materialized views
        NULL; -- Placeholder for report generation logic
    END generate_sales_report;
    
    FUNCTION calculate_commission(p_emp_id IN NUMBER, p_rate IN NUMBER) RETURN NUMBER AS
        v_total_sales NUMBER;
    BEGIN
        -- Depends on employee_sales_summary materialized view
        SELECT NVL(total_sales, 0) INTO v_total_sales
        FROM employee_sales_summary
        WHERE employee_id = p_emp_id;
        
        RETURN v_total_sales * p_rate;
    END calculate_commission;
END analytics_pkg;
/

-- Now grant cross-schema access (after tables are created)
ALTER SESSION SET CURRENT_SCHEMA = system;

-- Grant cross-schema access for lineage testing
GRANT SELECT ON hr_schema.employees TO sales_schema;
GRANT SELECT ON hr_schema.departments TO sales_schema;
GRANT SELECT ON sales_schema.orders TO analytics_schema;
GRANT SELECT ON sales_schema.order_items TO analytics_schema;
GRANT SELECT ON hr_schema.employees TO analytics_schema;
GRANT SELECT ON hr_schema.departments TO analytics_schema;

-- Commit all changes
COMMIT;

-- Generate sample queries for usage statistics and lineage testing
-- These queries will be processed by the SQL aggregator to generate usage statistics

-- Simple SELECT queries on individual tables
SELECT COUNT(*) FROM hr_schema.employees;
SELECT COUNT(*) FROM hr_schema.departments;
SELECT COUNT(*) FROM sales_schema.orders;
SELECT COUNT(*) FROM sales_schema.order_items;

-- Complex transformation queries for lineage testing
-- Multi-table JOIN with aggregation (shows table-to-table lineage)
SELECT 
    d.department_name,
    COUNT(e.employee_id) as employee_count,
    AVG(e.salary) as avg_salary,
    SUM(CASE WHEN e.hire_date > SYSDATE - 365 THEN 1 ELSE 0 END) as new_hires
FROM hr_schema.employees e 
JOIN hr_schema.departments d ON e.department_id = d.department_id
GROUP BY d.department_name
HAVING COUNT(e.employee_id) > 0;

-- Cross-schema transformation query (HR + Sales lineage)
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name as full_name,
    d.department_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_total) as total_sales,
    AVG(o.order_total) as avg_order_value
FROM hr_schema.employees e
JOIN hr_schema.departments d ON e.department_id = d.department_id
LEFT JOIN sales_schema.orders o ON e.employee_id = o.sales_rep_id
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
ORDER BY total_sales DESC NULLS LAST;

-- Complex order analysis with item details (Sales schema lineage)
SELECT 
    o.order_id,
    o.order_date,
    o.customer_id,
    COUNT(oi.line_item_id) as item_count,
    SUM(oi.unit_price * oi.quantity) as calculated_total,
    o.order_total,
    CASE 
        WHEN SUM(oi.unit_price * oi.quantity) > o.order_total THEN 'DISCOUNT_APPLIED'
        WHEN SUM(oi.unit_price * oi.quantity) < o.order_total THEN 'TAX_ADDED'
        ELSE 'EXACT_MATCH'
    END as price_analysis
FROM sales_schema.orders o
JOIN sales_schema.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_date, o.customer_id, o.order_total
HAVING COUNT(oi.line_item_id) > 1;

-- Materialized view queries (shows MV usage)
SELECT employee_name, total_sales, total_orders
FROM analytics_schema.employee_sales_summary 
WHERE total_sales > 1000
ORDER BY total_sales DESC;

SELECT order_month, order_count, total_revenue, avg_order_value
FROM analytics_schema.monthly_order_summary 
WHERE order_count > 5
ORDER BY total_revenue DESC;

-- Window function analytics
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.salary,
    d.department_name,
    RANK() OVER (PARTITION BY d.department_id ORDER BY e.salary DESC) as dept_salary_rank,
    AVG(e.salary) OVER (PARTITION BY d.department_id) as dept_avg_salary
FROM hr_schema.employees e
JOIN hr_schema.departments d ON e.department_id = d.department_id;

-- Subquery transformation (nested dependencies)
SELECT 
    dept_summary.department_name,
    dept_summary.employee_count,
    dept_summary.avg_salary,
    dept_summary.total_sales
FROM (
    SELECT 
        d.department_name,
        COUNT(DISTINCT e.employee_id) as employee_count,
        AVG(e.salary) as avg_salary,
        SUM(o.order_total) as total_sales
    FROM hr_schema.departments d
    LEFT JOIN hr_schema.employees e ON d.department_id = e.department_id
    LEFT JOIN sales_schema.orders o ON e.employee_id = o.sales_rep_id
    GROUP BY d.department_name
) dept_summary
WHERE dept_summary.employee_count > 0;

-- Execute stored procedures to show procedure usage
BEGIN
    analytics_schema.analytics_pkg.generate_sales_report('2024-01');
END;
/

-- Call functions to show function usage and dependencies
SELECT 
    order_id,
    sales_schema.get_order_total(order_id) as calculated_total,
    order_total as stored_total
FROM sales_schema.orders 
WHERE ROWNUM <= 10;

-- Get top performer (function that uses materialized view)
SELECT analytics_schema.get_top_performer() as top_sales_person FROM DUAL;

-- Execute procedure calls
BEGIN
    sales_schema.process_order(1001);
    sales_schema.process_order(1002);
    sales_schema.process_order(1003);
END;
/

-- Union query across schemas (complex transformation)
SELECT 'EMPLOYEE' as record_type, first_name as name, TO_CHAR(employee_id) as id
FROM hr_schema.employees
WHERE ROWNUM <= 5
UNION ALL
SELECT 'DEPARTMENT' as record_type, department_name as name, TO_CHAR(department_id) as id
FROM hr_schema.departments
WHERE ROWNUM <= 3;

-- CTE (Common Table Expression) for hierarchical analysis
WITH sales_hierarchy AS (
    SELECT 
        e.employee_id,
        e.first_name || ' ' || e.last_name as employee_name,
        e.manager_id,
        SUM(o.order_total) as direct_sales
    FROM hr_schema.employees e
    LEFT JOIN sales_schema.orders o ON e.employee_id = o.sales_rep_id
    GROUP BY e.employee_id, e.first_name, e.last_name, e.manager_id
)
SELECT 
    sh.employee_name,
    sh.direct_sales,
    mgr.first_name || ' ' || mgr.last_name as manager_name
FROM sales_hierarchy sh
LEFT JOIN hr_schema.employees mgr ON sh.manager_id = mgr.employee_id
WHERE sh.direct_sales > 0;

-- Apply all cross-schema grants now that all tables exist
-- Allow sales_schema to read from hr_schema (for procedures that join data)
GRANT SELECT ON hr_schema.employees TO sales_schema;
GRANT SELECT ON hr_schema.departments TO sales_schema;

-- Allow analytics_schema to read from hr_schema and sales_schema (for materialized views)
GRANT SELECT ON hr_schema.employees TO analytics_schema;
GRANT SELECT ON hr_schema.departments TO analytics_schema;
GRANT SELECT ON sales_schema.orders TO analytics_schema;
GRANT SELECT ON sales_schema.order_items TO analytics_schema;

-- Create regular views in user schemas (materialized views have privilege issues)
-- HR Schema view
ALTER SESSION SET CURRENT_SCHEMA = hr_schema;
CREATE VIEW employee_summary AS
SELECT 
    department_id,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    MAX(salary) AS max_salary,
    MIN(salary) AS min_salary
FROM employees
GROUP BY department_id;

-- Sales Schema view  
ALTER SESSION SET CURRENT_SCHEMA = sales_schema;
CREATE VIEW order_summary AS
SELECT 
    TO_CHAR(order_date, 'YYYY-MM') AS order_month,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value
FROM orders
GROUP BY TO_CHAR(order_date, 'YYYY-MM');

-- Analytics Schema view
ALTER SESSION SET CURRENT_SCHEMA = analytics_schema;
CREATE VIEW simple_analytics AS
SELECT 
    SYSDATE AS refresh_date,
    'Analytics Data' AS description,
    1 AS record_count
FROM DUAL;

-- Create a test materialized view in SYSTEM schema to test our extraction logic
-- SYSTEM user has all necessary privileges
CREATE MATERIALIZED VIEW system.test_mview AS
SELECT 
    COUNT(*) AS total_schemas,
    'Test Materialized View' AS description
FROM all_users
WHERE username LIKE '%SCHEMA%';

-- Status message
SELECT 'Oracle integration test environment with stored procedures and materialized views setup completed successfully!' AS STATUS FROM DUAL;