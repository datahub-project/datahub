### Configuration Notes

#### OpenLineage Namespace Mapping (Optional)

**Optional configuration** to map OpenLineage namespace URIs to DataHub platform information. Without this, the connector extracts platform type from URIs with default environment.

**Fields:**

- **`platform_instance`**: Platform instance identifier (must match source ingestion)
- **`database`** / **`schema`**: Defaults for incomplete dataset names from OpenLineage
  - 3-tier platforms (Snowflake, Postgres, Redshift): `database.schema.table`
  - 2-tier platforms (MySQL, Hive): `schema.table`
- **`convert_urns_to_lowercase`**: Normalize URNs to lowercase (use `true` for Snowflake)
- **`env`**: Environment tag (PROD, DEV, etc.)

**Fallback behavior**: Unmapped namespaces extract platform type from the URI (e.g., `postgresql://...` → `postgres`) without platform instance assignment.

#### SQL Parsing for Column-Level Lineage

Enable `parse_sql_for_lineage: true` to parse SQL queries from OpenLineage events for additional column-level lineage.

**Requirements:**

- DataHub graph connection configured
- `include_lineage: true`
- Schema information in OpenLineage events

**Limitations:**

- SQL dialect must be supported by sqlglot
- Complex SQL may fail to parse (skipped with warning)

#### Platform-Specific Handling

**Snowflake:** Use `convert_urns_to_lowercase: true` in namespace mapping

**BigQuery:** 3-tier naming (`project.dataset.table`). Set `database: project-id`, `schema: dataset-name`

**MySQL / 2-tier:** 2-tier naming (`schema.table`). Set `schema` only

**Postgres / Redshift:** 3-tier naming (`database.schema.table`). Set both `database` and `schema`

### Troubleshooting

#### Lineage Not Showing Up

1. Verify namespace mapping matches source ingestion platform instances
2. Check logs for `Processing OpenLineage event` messages
3. Confirm dataset names in OpenLineage match actual tables

#### Column-Level Lineage Missing

1. Enable `include_column_lineage: true`
2. Try `parse_sql_for_lineage: true` (requires DataHub graph connection)

#### Execution History Not Appearing

1. Enable `include_pipeline_executions: true`
2. Adjust `execution_start_days_ago` if needed
3. Verify API permissions

#### Performance Issues

1. Reduce `execution_start_days_ago`
2. Use `project_pattern` to filter projects
3. Disable `include_pipeline_executions` or `include_streaming_pipelines`
4. Increase `api_config.request_timeout_sec` if needed
