### Configuration Notes

#### OpenLineage Namespace Mapping

The `namespace_to_platform_instance` configuration maps OpenLineage namespace URIs to DataHub platform information. This is critical for connecting lineage from Matillion pipelines to your existing datasets.

**Configuration fields:**

- **`platform_instance`**: Platform instance identifier for the data source

  - When set, DataHub prepends this to the dataset name in the final URN
  - Example: `platform_instance.schema.table` or `platform_instance.table` depending on platform type
  - Must match the platform instance used when ingesting the source data platform

- **`database` and `schema`**: Handle incomplete dataset names from OpenLineage events

  - **3-tier platforms** (Snowflake, Postgres, Redshift): Use `database.schema.table` format
    - Without platform_instance: `table` → `database.schema.table`
    - Without platform_instance: `schema.table` → `database.schema.table`
    - With platform_instance: `table` → `schema.table` (platform_instance is prepended by URN builder)
    - With platform_instance: `schema.table` → `schema.table` (no change)
  - **2-tier platforms** (MySQL, Hive): Use `schema.table` format
    - Without platform_instance: `table` → `schema.table`
    - With platform_instance: `table` → `table` (platform_instance is prepended by URN builder)
  - **Defaults** only apply when the dataset name from OpenLineage doesn't include that context

- **`convert_urns_to_lowercase`**: Whether to normalize dataset URNs to lowercase

  - Set to `true` for case-insensitive platforms like Snowflake
  - Set to `false` for case-sensitive platforms
  - Default: `false`

- **`env`**: Environment tag for datasets (PROD, DEV, STAGING, etc.)
  - Default: `PROD`
  - Should match the environment used in your existing dataset URNs

**Example for unmapped namespaces:**

If an OpenLineage namespace is not explicitly configured in `namespace_to_platform_instance`, the connector will:

1. Extract the platform type from the namespace URI (e.g., `postgresql://...` → `postgres`)
2. Use default environment (`PROD`)
3. Not apply any database/schema defaults
4. Not assign a platform instance

This fallback behavior allows lineage extraction even for ad-hoc or development data sources, but may not correctly link to existing datasets if platform instances are used.

#### SQL Parsing for Column-Level Lineage

When `parse_sql_for_lineage: true` is enabled, DataHub uses the `SqlParsingAggregator` to parse SQL queries extracted from OpenLineage events. This provides additional column-level lineage beyond what's explicitly provided in the OpenLineage column lineage data.

**How it works:**

1. SQL queries are extracted from the `sql` field in OpenLineage run events
2. Schemas of input/output datasets are registered with the aggregator (from OpenLineage schema info)
3. SQL is parsed using platform-specific SQL dialects to infer column-level dependencies
4. Parsed lineage is combined with explicit OpenLineage column lineage
5. Final lineage is emitted to DataHub

**Requirements:**

- DataHub graph connection configured (for schema lookups)
- `include_lineage: true` (to extract OpenLineage events)
- Input/output datasets must have schema information in OpenLineage events

**Limitations:**

- SQL dialect must be supported by DataHub's SQL parser (sqlglot)
- Complex SQL with unsupported syntax may fail to parse (gracefully skipped with warning)
- Cross-platform transformations (e.g., Postgres SQL writing to Snowflake) project correctly as long as schema information is available

#### Platform-Specific Handling

**Snowflake:**

- Automatically lowercases schema field names for schema field URNs (Snowflake normalizes identifiers to uppercase, but DataHub stores them lowercase)
- Use `convert_urns_to_lowercase: true` in namespace mapping to match standard Snowflake dataset ingestion
- Database context in table names is omitted when `platform_instance` is set (URN builder handles it)

**BigQuery:**

- Uses 3-tier naming: `project.dataset.table`
- Set `database: project-id` and `schema: dataset-name` in namespace mapping
- Platform instance should match your BigQuery source ingestion

**MySQL / 2-tier platforms:**

- Uses 2-tier naming: `schema.table` (no database level)
- Only set `schema` in namespace mapping, leave `database` unset
- System automatically detects 2-tier platforms and adjusts normalization logic

**Postgres / Redshift:**

- Uses 3-tier naming: `database.schema.table`
- Set both `database` and `schema` in namespace mapping
- Default schema is often `public` for Postgres

### Troubleshooting

#### Lineage Not Showing Up

**1. Check namespace mapping configuration:**

Verify that OpenLineage namespace URIs are correctly mapped to platform instances:

```yaml
namespace_to_platform_instance:
  "postgresql://your-actual-host.rds.amazonaws.com:5432":
    platform_instance: postgres_prod # Must match your postgres source ingestion
    env: PROD
```

**2. Verify platform instance consistency:**

Ensure the `platform_instance` values in your Matillion recipe match exactly what you used when ingesting the source data platforms. You can check existing dataset URNs in DataHub:

```
urn:li:dataset:(urn:li:dataPlatform:postgres,postgres_prod.public.users,PROD)
                                             ^^^^^^^^^^^^^^^^
                                             This is the platform_instance
```

**3. Check OpenLineage events:**

Enable debug logging to see raw OpenLineage events:

```yaml
source:
  config:
    # ... other config
```

Then check logs for `Processing OpenLineage event` messages to verify events are being received and parsed.

**4. Verify dataset names:**

Check that dataset names in OpenLineage match your actual table names. Use debug logging to inspect normalized dataset names.

#### Column-Level Lineage Missing

**1. Enable column lineage extraction:**

```yaml
include_column_lineage: true
```

**2. Check if OpenLineage provides column lineage:**

Not all Matillion components emit column-level lineage in OpenLineage events. This is a Matillion API limitation.

**3. Try SQL parsing:**

If OpenLineage doesn't include column lineage, enable SQL parsing:

```yaml
parse_sql_for_lineage: true
```

This requires a DataHub graph connection and may not work for all SQL dialects.

#### Execution History Not Appearing

**1. Enable execution ingestion:**

```yaml
include_pipeline_executions: true
```

**2. Check execution lookback window:**

```yaml
execution_start_days_ago: 7 # Default is 7 days
```

**3. Verify API permissions:**

Ensure your API token has permissions to read execution history.

#### Performance Issues

**1. Reduce execution lookback period:**

```yaml
execution_start_days_ago: 1 # Only ingest last 24 hours
```

**2. Filter specific projects:**

```yaml
project_pattern:
  allow:
    - "production_.*"
```

**3. Disable unused features:**

```yaml
include_pipeline_executions: false
include_streaming_pipelines: false
```

**4. Increase timeout for large environments:**

```yaml
api_config:
  request_timeout_seconds: 300 # Default is 300 (5 minutes)
```
