### Overview

This source plugin extracts metadata from StarRocks, including schema information for tables and views across all catalogs. It discovers the full three-tier hierarchy (Catalog -> Database -> Table) and supports both internal and external catalogs (Hive, Iceberg, Hudi, Delta Lake).

### Prerequisites

You need to grant the following privileges to the ingestion user:

```sql
GRANT USAGE ON ALL CATALOGS TO USER 'datahub'@'%';
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'datahub'@'%';
```

For more details on StarRocks privileges, see the [GRANT documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/).

If the user has partial privileges (e.g., access to only specific catalogs or databases), the connector will only ingest metadata from the accessible resources. Use `catalog_pattern` and `schema_pattern` to align ingestion scope with user permissions.

#### Terminology

Due to naming convention differences between StarRocks and DataHub, `schema_pattern` filters StarRocks databases (not schemas) and `catalog_pattern` filters StarRocks catalogs.
