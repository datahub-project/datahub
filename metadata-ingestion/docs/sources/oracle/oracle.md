The Oracle source extracts metadata from Oracle databases, including:

- **Tables and Views**: Standard relational tables and views with column information, constraints, and comments
- **Stored Procedures**: Functions, procedures, and packages with source code, arguments, and dependency tracking
- **Materialized Views**: Materialized views with proper lineage and refresh information
- **Lineage**: Automatic lineage generation from stored procedure definitions and materialized view queries via SQL parsing
- **Usage Statistics**: Query execution statistics and table access patterns (when audit data is available)
- **Operations**: Data modification events (CREATE, INSERT, UPDATE, DELETE) from audit trail data

The connector uses the `python-oracledb` driver and supports both thin mode (default, no Oracle client required) and thick mode (requires Oracle client installation).

As a SQL-based service, the Oracle integration is also supported by our SQL profiler for table and column statistics.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
