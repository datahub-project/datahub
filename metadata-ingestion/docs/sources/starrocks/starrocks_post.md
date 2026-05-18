### Capabilities

#### Stateful Ingestion

Stateful ingestion is supported and tracks previously ingested entities. When enabled, it can automatically soft-delete entities that are no longer present in StarRocks. To use it, set a `pipeline_name` and enable stateful ingestion:

```yaml
pipeline_name: starrocks_ingestion
source:
  type: starrocks
  config:
    stateful_ingestion:
      enabled: true
```

#### Multi-Catalog Discovery

The connector discovers all catalogs by default, including external catalogs (Hive, Iceberg, Hudi, Delta Lake). You can control this with `include_external_catalogs` and `catalog_pattern`.

### Limitations

- Column-level lineage is not currently supported.
- Some external catalog table types may fail schema reflection depending on the StarRocks SQLAlchemy dialect version. These failures are logged as warnings and do not block ingestion of other tables.

### Troubleshooting

#### External catalog tables not appearing

If tables from external catalogs are missing, verify that the ingestion user has `USAGE` privileges on those catalogs and `SELECT` on the tables. Check the ingestion logs for warnings about specific databases or tables that failed during reflection.

#### Column type warnings

StarRocks supports complex types (e.g., `ARRAY(JSON())`) that may not map directly to DataHub types. These columns will still be ingested but their parsed field type will not be populated.
