### Prerequisites

In order to execute this source, you need to grant the following privileges:

```sql
GRANT USAGE ON ALL CATALOGS TO USER 'datahub'@'%';
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'datahub'@'%';
```

For more details on StarRocks privileges, see the [GRANT documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/).

**Note**: If the user has partial privileges (e.g., access to only specific catalogs or databases), the connector will only ingest metadata from the accessible resources. Use `catalog_pattern` and `schema_pattern` to align ingestion scope with user permissions.

### Configuration Notes

**Three-Tier Hierarchy**: StarRocks uses a Catalog → Database → Table hierarchy. The connector discovers all catalogs and databases by default, including external catalogs (Hive, Iceberg, Hudi, Delta Lake).
This can be controlled through the configuration variable `include_external_catalogs`.

**Terminology**: Due to naming convention differences, `schema_pattern` filters StarRocks databases (not schemas) and `catalog_pattern` filters StarRocks catalogs.
