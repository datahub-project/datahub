The Oracle source extracts metadata from Oracle databases, including:

- **Tables and Views**: Standard relational tables and views with column information, constraints, and comments
- **Stored Procedures**: Functions, procedures, and packages with source code, arguments, and dependency tracking
- **Materialized Views**: Materialized views with proper lineage and refresh information
- **Lineage**: Automatic lineage generation from stored procedure definitions and materialized view queries via SQL parsing
- **Usage Statistics**: Query execution statistics and table access patterns (when audit data is available)
- **Operations**: Data modification events (CREATE, INSERT, UPDATE, DELETE) from audit trail data

The Oracle integration supports both thin mode (default, no Oracle client required) and thick mode (requires Oracle client installation) connections using the modern `python-oracledb` driver.

As a SQL-based service, the Oracle integration is also supported by our SQL profiler for table and column statistics.
