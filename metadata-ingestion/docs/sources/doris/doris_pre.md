### Overview

The `doris` module ingests metadata from Doris into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Profiling

Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically excluded from field-level profiling as they don't support standard aggregation operations. Table-level statistics are still collected for all tables.

#### Stored Procedures

Stored procedure ingestion is disabled by default because Doris's `information_schema.ROUTINES` table is always empty.

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

#### Multi-Catalog (Iceberg / Hive / Paimon)

Doris organizes metadata as `catalog.database.table`. The built-in catalog is `internal`. External catalogs (Iceberg, Hive, Paimon, JDBC, …) need an explicit catalog context.

Set `catalog` to the Doris catalog name, or pass `database` as `catalog.database`:

```yaml
source:
  type: doris
  config:
    host_port: "doris-fe:9030"
    username: datahub
    password: "..."
    catalog: iceberg_catalog
    # Optional: limit to one database in that catalog
    # database: db_ods
    # Recommended when the same database names exist in other catalogs
    platform_instance: iceberg_catalog
```

Without `catalog`, the source uses the session catalog (usually `internal`). If your connection string already selects an external catalog (for example `sqlalchemy_uri` ending in `/iceberg_catalog.db_ods`), the source detects `CURRENT_CATALOG()` and reconnects with the fully qualified `catalog.database` path.
