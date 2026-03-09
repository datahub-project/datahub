### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

DataHub can extract table-level lineage from your PostgreSQL query history using the `pg_stat_statements` extension. This feature analyzes executed SQL queries to automatically discover upstream and downstream dataset dependencies.

### Limitations

1. **Historical data only**

   - Lineage is extracted from executed queries, not from schema definitions
   - Queries must have been executed since the last `pg_stat_statements_reset()`

2. **Dynamic SQL**

   - Parameterized queries show parameter placeholders, not actual values
   - Example: `SELECT * FROM users WHERE id = $1` (value not captured)

3. **Complex transformations**

   - The extractor may not parse extremely complex queries with nested CTEs or exotic syntax
   - Failed queries are logged but don't block ingestion

4. **No column-level lineage**
   - Currently supports table-level lineage only
   - Column-level lineage may be added in future releases

#### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
