-- Post-setup script to verify the test environment
-- This script runs after the main setup and verifies everything is working

-- Connect as SYSTEM to check the setup
CONNECT SYSTEM/example@localhost:1521/XEPDB1;

-- Verify schemas exist
SELECT username, created, account_status 
FROM dba_users 
WHERE username IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA', 'STAGING_SCHEMA')
ORDER BY username;

-- Verify objects were created
SELECT owner, object_type, COUNT(*) as object_count
FROM dba_objects 
WHERE owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA', 'STAGING_SCHEMA')
AND object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY')
GROUP BY owner, object_type
ORDER BY owner, object_type;

-- Verify stored procedures and functions
SELECT owner, object_name, object_type, status
FROM dba_objects 
WHERE owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
AND object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')
ORDER BY owner, object_type, object_name;

-- Verify materialized views
SELECT owner, mview_name, refresh_mode, refresh_method
FROM dba_mviews 
WHERE owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
ORDER BY owner, mview_name;

-- Verify dependencies exist
SELECT owner, name, type, referenced_owner, referenced_name, referenced_type
FROM dba_dependencies 
WHERE owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
AND referenced_owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
ORDER BY owner, name;

-- Test a simple function call that actually exists
DECLARE
    v_salary NUMBER;
BEGIN
    v_salary := hr_schema.calculate_annual_salary(100);
    DBMS_OUTPUT.PUT_LINE('Annual salary calculation test: ' || v_salary);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Function test failed: ' || SQLERRM);
END;
/

-- Execute sample queries to populate V$SQL for query usage testing
-- These queries will be captured by include_query_usage feature
ALTER SESSION SET CURRENT_SCHEMA = HR_SCHEMA;

-- Simple SELECT queries
SELECT employee_id, first_name, last_name, salary FROM hr_schema.employees WHERE department_id = 10;
SELECT department_name, location_id FROM hr_schema.departments WHERE department_id < 100;
SELECT e.employee_id, e.last_name, d.department_name FROM hr_schema.employees e JOIN hr_schema.departments d ON e.department_id = d.department_id;

-- DML operations to test table-to-table lineage
-- Create staging tables in dedicated STAGING_SCHEMA to avoid cluttering test configs
ALTER SESSION SET CURRENT_SCHEMA = STAGING_SCHEMA;

-- Create employee backup table with INSERT...SELECT to show lineage
CREATE TABLE employee_backup AS SELECT * FROM hr_schema.employees WHERE 1=0;

INSERT INTO staging_schema.employee_backup (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id
FROM hr_schema.employees
WHERE department_id = 10;

-- Create reporting table with CTAS to show lineage
CREATE TABLE high_earners AS
SELECT e.employee_id, e.first_name, e.last_name, e.salary, d.department_name
FROM hr_schema.employees e
JOIN hr_schema.departments d ON e.department_id = d.department_id
WHERE e.salary > 15000;

ALTER SESSION SET CURRENT_SCHEMA = HR_SCHEMA;

ALTER SESSION SET CURRENT_SCHEMA = SALES_SCHEMA;

-- Sales schema queries (fixed to match actual schema)
SELECT order_id, customer_id, order_date FROM sales_schema.orders WHERE order_date > SYSDATE - 30;
SELECT order_id, line_item_id, unit_price FROM sales_schema.order_items WHERE unit_price > 100;

ALTER SESSION SET CURRENT_SCHEMA = STAGING_SCHEMA;

-- Create order analytics table with cross-schema lineage (fixed column names)
CREATE TABLE order_analytics AS
SELECT o.order_id, o.customer_id, o.order_date, o.order_total, 
       e.first_name || ' ' || e.last_name as sales_rep_name
FROM sales_schema.orders o
JOIN hr_schema.employees e ON o.sales_rep_id = e.employee_id;

-- Create empty table for aggregated data showing table-to-table lineage
CREATE TABLE daily_revenue (
    order_date DATE NOT NULL,
    total_revenue NUMBER(10,2)
);

-- Insert aggregated data (this INSERT will be captured in V$SQL for lineage)
INSERT INTO staging_schema.daily_revenue (order_date, total_revenue)
SELECT order_date, SUM(order_total)
FROM sales_schema.orders
GROUP BY order_date;

-- COMMIT to ensure queries are flushed to V$SQL
COMMIT;

ALTER SESSION SET CURRENT_SCHEMA = ANALYTICS_SCHEMA;
SELECT refresh_date, description FROM analytics_schema.simple_analytics WHERE record_count > 0;

-- Note: V$SQL is a cache, not a persistent query log
-- The DML queries executed above (INSERT INTO staging_schema.employee_backup, 
-- INSERT INTO staging_schema.daily_revenue) may or may not be in V$SQL when the 
-- connector runs, depending on Oracle's cache behavior.
-- The test validates that include_query_usage works and extracts any queries present in V$SQL.
-- This is the expected behavior for V$SQL-based query usage extraction.
-- Staging tables are in STAGING_SCHEMA to avoid cluttering test configurations.

-- Display final status
SELECT 'Oracle integration test environment verification completed successfully!' AS status FROM dual;

-- Show connection info for reference
SELECT 
    'Connect with: sqlplus hr_schema/hr123@localhost:1521/XEPDB1' AS hr_connection,
    'Connect with: sqlplus sales_schema/sales123@localhost:1521/XEPDB1' AS sales_connection,
    'Connect with: sqlplus analytics_schema/analytics123@localhost:1521/XEPDB1' AS analytics_connection,
    'Connect with: sqlplus staging_schema/staging123@localhost:1521/XEPDB1' AS staging_connection
FROM dual;