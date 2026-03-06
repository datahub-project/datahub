### Overview

The `doris` module ingests metadata from Doris into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### Doris Version

Doris 3.0.x is required. Doris 2.0+ may work but is untested.

#### Required Permissions

Your Doris user requires specific privileges to extract metadata.

```sql
-- Create user
CREATE USER 'datahub'@'%' IDENTIFIED BY 'your_password';

-- Grant required privileges
GRANT SELECT_PRIV ON *.* TO 'datahub'@'%';
GRANT SHOW_VIEW_PRIV ON *.* TO 'datahub'@'%';
```

- `SELECT_PRIV`: Required for table and column metadata
- `SHOW_VIEW_PRIV`: Required for view definitions and lineage

### Profiling

Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically excluded from field-level profiling as they don't support standard aggregation operations. Table-level statistics are still collected for all tables.

### Stored Procedures

Stored procedure ingestion is disabled by default because Doris's `information_schema.ROUTINES` table is always empty.
