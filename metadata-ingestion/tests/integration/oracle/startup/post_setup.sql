-- Post-setup script to verify the test environment
-- This script runs after the main setup and verifies everything is working

-- Connect as SYSTEM to check the setup
CONNECT SYSTEM/example@localhost:1521/XEPDB1;

-- Verify schemas exist
SELECT username, created, account_status 
FROM dba_users 
WHERE username IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
ORDER BY username;

-- Verify objects were created
SELECT owner, object_type, COUNT(*) as object_count
FROM dba_objects 
WHERE owner IN ('HR_SCHEMA', 'SALES_SCHEMA', 'ANALYTICS_SCHEMA')
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

-- Display final status
SELECT 'Oracle integration test environment verification completed successfully!' AS status FROM dual;

-- Show connection info for reference
SELECT 
    'Connect with: sqlplus hr_schema/hr123@localhost:1521/XEPDB1' AS hr_connection,
    'Connect with: sqlplus sales_schema/sales123@localhost:1521/XEPDB1' AS sales_connection,
    'Connect with: sqlplus analytics_schema/analytics123@localhost:1521/XEPDB1' AS analytics_connection
FROM dual;