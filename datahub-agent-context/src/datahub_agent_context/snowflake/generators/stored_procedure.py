"""Generate Snowflake stored procedure SQL."""


def generate_stored_procedure_sql() -> str:
    """Generate stored procedure SQL that uses configuration variables."""
    return """-- ============================================================================
-- Step 3: Create JavaScript Stored Procedure for SQL Execution
-- ============================================================================
-- This creates a stored procedure that can execute SELECT queries dynamically
-- and return results as JSON
--
-- Prerequisites:
-- - Run 00_configuration.sql first to set variables
-- ============================================================================

USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);
USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);

-- Create the JavaScript stored procedure (using CREATE OR REPLACE, so no need to drop first)
CREATE OR REPLACE PROCEDURE EXECUTE_DYNAMIC_SQL(SQL_TEXT STRING)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Validate that it's a SELECT query only
  var queryUpper = SQL_TEXT.trim().toUpperCase();
  if (!queryUpper.startsWith('SELECT')) {
    return {
      "success": false,
      "error": "Only SELECT queries are allowed. Query must start with SELECT.",
      "status": 400
    };
  }

  try {
    var stmt = snowflake.createStatement({ sqlText: SQL_TEXT });
    var rs = stmt.execute();

    var results = [];
    var columns = [];

    // Get column names from the first row metadata
    var columnCount = rs.getColumnCount();
    for (var i = 1; i <= columnCount; i++) {
      columns.push(rs.getColumnName(i));
    }

    // Process all rows
    while (rs.next()) {
      var row = {};
      for (var i = 1; i <= columnCount; i++) {
        var colName = rs.getColumnName(i);
        var colValue = rs.getColumnValue(i);
        // Handle null values and convert to JSON-serializable format
        row[colName] = colValue;
      }
      results.push(row);
    }

    return {
      "success": true,
      "columns": columns,
      "rows": results,
      "row_count": results.length
    };

  } catch (err) {
    return {
      "success": false,
      "error": err.message,
      "status": 500
    };
  }
$$;

-- Grant usage on the procedure
GRANT USAGE ON PROCEDURE EXECUTE_DYNAMIC_SQL(STRING) TO ROLE IDENTIFIER($SF_ROLE);

-- Verify it was created
DESCRIBE PROCEDURE EXECUTE_DYNAMIC_SQL(STRING);

SELECT
    'Stored procedure EXECUTE_DYNAMIC_SQL created successfully!' AS status,
    $SF_DATABASE || '.' || $SF_SCHEMA || '.EXECUTE_DYNAMIC_SQL' AS procedure_name;
"""
