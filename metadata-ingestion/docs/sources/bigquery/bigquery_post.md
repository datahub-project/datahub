### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage and Usage Computation Details

DataHub's BigQuery connector supports two approaches for extracting lineage and usage statistics:

##### Modern Approach (Default): `use_queries_v2: true`

**Recommended for most users** - Uses BigQuery's Information Schema for efficient metadata extraction.

- **Data Source**: BigQuery Information Schema (`INFORMATION_SCHEMA.JOBS*` tables)
- **Features**:
  - Advanced lineage extraction using SQL parsing
  - Query entities with full query text
  - Query popularity statistics and rankings
  - Multi-region support via `region_qualifiers`
  - Table and column-level usage statistics
  - User filtering pushdown for performance (see [User Email Filtering Pushdown](#user-email-filtering-pushdown-performance-optimization) section below)
- **Requirements**:
  - `bigquery.jobs.listAll` permission on target projects
  - No additional Cloud Logging permissions needed

**Configuration**:

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: true # Default
    include_queries: true # Enable query entities
    include_query_usage_statistics: true # Query popularity stats
    region_qualifiers: ["region-us", "region-eu"] # Multi-region support
```

##### User Email Filtering Pushdown (Performance Optimization)

The `pushdown_deny_usernames` and `pushdown_allow_usernames` options push user filtering directly to BigQuery's SQL query, reducing data transfer and improving performance for large query volumes.

**When to Use:**

- You have large query volumes (>10k queries in your time window)
- You want to exclude high-volume service accounts or bots
- You want to reduce BigQuery data transfer costs
- You want to reduce overall DataHub ingestion time

**Example Configuration:**

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: true # Required for pushdown
    pushdown_deny_usernames:
      - "bot_%"
      - "%@%.iam.gserviceaccount.com" # Exclude service accounts
    pushdown_allow_usernames:
      - "analyst_%@example.com"
      - "data_%@example.com"
```

**Behavior:**

- When patterns are configured: Filtering happens server-side with BigQuery SQL using case-insensitive `LIKE`
- When empty (default): No server-side filtering; use `usage.user_email_pattern` for client-side filtering
- Patterns use SQL LIKE syntax (`%` = any characters, `_` = single character)
- Matching is case-insensitive (e.g., `bot_%` matches `Bot_User@example.com`)
- If a user matches both allow AND deny patterns, deny takes precedence (user is excluded)

**Prerequisites:**

- `use_queries_v2: true` must be enabled (default)
- Patterns must be valid SQL LIKE patterns

**Note:** These configs are independent from `usage.user_email_pattern`. The pushdown filters are applied at the SQL query level for performance, while `user_email_pattern` is applied client-side during processing.

##### Legacy Approach: `use_queries_v2: false`

**Use when you need specific legacy features** - Processes BigQuery audit logs for metadata extraction.

- **Data Source**: BigQuery audit logs (two options below)
- **Features**:
  - Basic table-level lineage and usage statistics
  - `upstream_lineage_in_report` debugging feature
  - Works with existing audit log exports

**Two data source options**:

##### Option 1: Google Cloud Logging API (Default)

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: false
    use_exported_bigquery_audit_metadata: false # Default
```

- **Requirements**: `logging.logEntries.list` and `logging.privateLogEntries.list` permissions
- **Limitations**: API rate limits, potential costs for large volumes

##### Option 2: Pre-exported Audit Logs in BigQuery Tables

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: false
    use_exported_bigquery_audit_metadata: true
    bigquery_audit_metadata_datasets:
      - "my-project.audit_dataset"
      - "another-project.audit_logs"
```

- **Requirements**:
  - Pre-exported audit logs in BigQuery tables
  - Tables must be named `cloudaudit_googleapis_com_data_access`
  - Only protoPayloads with `type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata` are supported
- **Benefits**: No Cloud Logging API limits, better for large-scale ingestion
- **Setup**: Follow [BigQuery audit logs export guide](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)
- **Note**: The `bigquery_audit_metadata_datasets` parameter accepts datasets in `$PROJECT.$DATASET` format, allowing lineage computation from multiple projects.

#### Profiling Details

:::note Profiling Permission Requirement

When profiling is enabled, the `bigquery.tables.getData` permission is **required**. This is needed to access detailed table metadata including partition information. See the permissions section above for details.

:::

For performance reasons, we only profile the latest partition for partitioned tables and the latest shard for sharded tables.
You can set partition explicitly with `partition.partition_datetime` property if you want, though note that partition config will be applied to all partitioned tables.

#### Caveats

- For materialized views, lineage is dependent on logs being retained. If your GCP logging is retained for 30 days (default) and 30 days have passed since the creation of the materialized view we won't be able to get lineage for them.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
