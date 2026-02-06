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

Try `parse_sql_for_lineage: true` (requires DataHub graph connection)

#### Execution History Not Appearing

1. Adjust `start_time` to query further back in time if needed
2. Verify API permissions for Pipeline Executions API

#### Performance Issues

1. Reduce time window by adjusting `start_time` (e.g., only last 7 days instead of 30)
2. Use filtering patterns to reduce scope:
   - `project_patterns` to filter projects
   - `environment_patterns` to filter environments
   - `pipeline_patterns` to filter pipelines
   - `streaming_pipeline_patterns` to filter streaming pipelines
3. Disable `include_streaming_pipelines` if not needed
4. Increase `api_config.request_timeout_sec` if needed

### Filtering Options

The connector supports flexible regex-based filtering to control what metadata is ingested:

#### Project Filtering

```yaml
project_patterns:
  allow: ["^prod-.*", "^staging-.*"]
  deny: [".*-deprecated$"]
```

#### Environment Filtering

```yaml
environment_patterns:
  allow: ["^production$", "^staging$"]
  deny: ["^sandbox.*"]
```

#### Pipeline Filtering

```yaml
pipeline_patterns:
  allow: [".*"]
  deny: ["^test_.*", ".*_backup$"]
```

#### Streaming Pipeline Filtering

```yaml
streaming_pipeline_patterns:
  allow: ["^cdc_.*"]
  deny: [".*_test$"]
```

**Note**: All patterns are case-insensitive by default and support full regex syntax. Deny patterns take precedence over allow patterns.

### Child Pipeline Dependencies

The connector automatically detects and tracks when pipelines call other pipelines (via "Run Pipeline" components). This creates step-level dependency relationships in DataHub, showing:

- Which pipeline steps trigger child pipelines
- Complete execution lineage across pipeline orchestrations
- Cross-pipeline data flow for comprehensive impact analysis

No configuration needed - this feature is automatic when execution history is ingested.

### Published vs Unpublished Pipelines

The connector can discover pipelines from two sources:

1. **Published Pipelines** - Pipelines explicitly published in Matillion DPC (fetched from `/published-pipelines` API)
2. **Unpublished Pipelines** - Pipelines discovered from recent execution history (fetched from `/pipeline-executions` API)

By default, both types are ingested. To only ingest published pipelines:

```yaml
include_unpublished_pipelines: false
```

This is useful when:

- You want to control what appears in DataHub via Matillion's publish workflow
- You have many development/test pipelines that run but shouldn't be documented
- You want to reduce ingestion time and API calls
