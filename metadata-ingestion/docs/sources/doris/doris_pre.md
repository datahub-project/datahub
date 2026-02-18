### Prerequisites

#### Doris Version

Doris 3.0.x is required. Doris 2.0+ may work but is untested.

Check your Doris version:

```sql
SELECT version();
```

#### Network Access

Ensure network connectivity from DataHub to Doris FE (Frontend) on port `9030`.

#### Required Permissions

The DataHub user requires the following minimal permissions:

```sql
-- Create user
CREATE USER 'datahub'@'%' IDENTIFIED BY 'your_password';

-- Grant required privileges
GRANT SELECT_PRIV ON *.* TO 'datahub'@'%';
GRANT SHOW_VIEW_PRIV ON *.* TO 'datahub'@'%';
```

`SELECT_PRIV` is required to extract table and column metadata. `SHOW_VIEW_PRIV` is required to extract view definitions and lineage.

### Profiling

If you enable profiling in your ingestion recipe, note that Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically excluded from field-level profiling as they don't support standard aggregation operations. Table-level statistics are still collected for all tables.

### Stored Procedures

Stored procedure ingestion is disabled by default because Doris's `information_schema.ROUTINES` table is always empty.
