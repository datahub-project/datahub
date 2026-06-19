# Datahub Airflow Plugin

See [the DataHub Airflow docs](https://docs.datahub.com/docs/lineage/airflow) for details.

## Version Compatibility

The plugin supports Apache Airflow 3.0+. Airflow 2.x is not supported — pin
`acryl-datahub-airflow-plugin <= 1.6.0` (the last release with Airflow 2 support)
if you need to integrate with Airflow 2.

| Airflow Version | Status             | Notes                |
| --------------- | ------------------ | -------------------- |
| 2.x             | ❌ Unsupported     | Use version <= 1.6.0 |
| 3.0+            | ✅ Fully Supported |                      |

## Installation

```bash
pip install acryl-datahub-airflow-plugin
```

This installs:

- `acryl-datahub[sql-parser,datahub-rest]` — DataHub SDK with SQL parsing and REST emitter
- `pydantic>=2.4.0`
- `apache-airflow>=3.0.0,<4.0.0`
- `apache-airflow-providers-openlineage>=2.1.0`

### Optional extras

```bash
pip install 'acryl-datahub-airflow-plugin[datahub-kafka]'   # Kafka emitter
pip install 'acryl-datahub-airflow-plugin[datahub-file]'    # File emitter (testing)
```

## Configuration

The plugin can be configured via `airflow.cfg` under the `[datahub]` section. Below are the key configuration options:

### Extractor Patching (OpenLineage Enhancements)

When `enable_extractors=True` (default), the DataHub plugin enhances OpenLineage extractors to provide better lineage. You can fine-tune these enhancements:

```ini
[datahub]
# Enable/disable all OpenLineage extractors
enable_extractors = True  # Default: True

# Enable multi-statement SQL parsing (resolves temp tables, merges lineage)
enable_multi_statement_sql_parsing = False  # Default: False

# Patch SQLParser to use DataHub's advanced SQL parser (enables column-level lineage)
patch_sql_parser = True  # Default: True

# Use DataHub's enhancements for specific operators
extract_athena_operator = True              # Default: True
extract_bigquery_insert_job_operator = True # Default: True
extract_teradata_operator = True            # Default: True
```

**Multi-Statement SQL Parsing:**

When `enable_multi_statement_sql_parsing=True`, if a task executes multiple SQL statements (e.g., `CREATE TEMP TABLE ...; INSERT ... FROM temp_table;`), DataHub parses all statements together and resolves temporary table dependencies within that task. By default (False), only the first statement is parsed.

**How patches work:**

The DataHub plugin monkey-patches OpenLineage extractors at runtime:

- `patch_sql_parser=True` patches `SQLParser.generate_openlineage_metadata_from_sql()` to use DataHub's parser, enabling more accurate lineage and column-level lineage.
- `extract_athena_operator` / `extract_bigquery_insert_job_operator` / `extract_teradata_operator` patch the corresponding operator's `get_openlineage_facets_on_complete()` method with DataHub's enhanced implementation.

### Example: disable DataHub's SQL parser

```ini
[datahub]
enable_extractors = True
patch_sql_parser = False
```

### Other Configuration Options

For a complete list of configuration options, see the [DataHub Airflow documentation](https://docs.datahub.com/docs/lineage/airflow#configuration).

## Developing

See the [developing docs](../../metadata-ingestion/developing.md).
