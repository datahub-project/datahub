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
    region_qualifiers: ["region-us", "region-eu"] # Regions to scan for INFORMATION_SCHEMA.JOBS
    region_qualifiers_auto_discovery: true # Set to true to auto-extend from discovered dataset locations (default: false)
```

##### Multi-Region Configuration

`INFORMATION_SCHEMA.JOBS` is scoped per region. By default DataHub scans `region-us` and `region-eu`. If your project has datasets in other regions (e.g. `europe-west1`, `asia-northeast1`), usage and lineage for those regions will be silently missing unless you configure additional regions.

Two options:

- **Auto-discovery** (recommended for multi-region projects): set `region_qualifiers_auto_discovery: true`. DataHub detects dataset locations during schema ingestion and merges any newly found regions into `region_qualifiers`. The configured `region_qualifiers` list is always used as the starting set — auto-discovery only adds to it, never removes from it.
- **Explicit list**: add regions directly to `region_qualifiers`, e.g. `["region-us", "region-eu", "region-asia-northeast1"]`.

:::info

`region_qualifiers_auto_discovery` defaults to `false` to avoid unexpected BigQuery query costs. Enable it only if you have datasets outside `region-us` / `region-eu`.

:::

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

#### BigQuery Sharing (Linked Datasets)

When you enable `include_linked_datasets`, DataHub identifies datasets in your subscriber projects that are Analytics Hub linked datasets — read-only mirrors of a publisher's source dataset — and surfaces them as a distinct subtype with cross-project lineage to the publisher.

For each linked dataset, the connector emits:

- A container with subtype `Linked Dataset` instead of `Dataset`.
- Governance metadata captured from Analytics Hub on the container's custom properties: source publisher project and dataset, listing or data exchange the subscription is bound to, subscription state (`STATE_ACTIVE`, `STATE_STALE`, `STATE_INACTIVE`), publisher organization, and timestamps.

For each table or view inside a linked dataset (when `include_linked_dataset_lineage` is also enabled, the default):

- A `Siblings` aspect linking the consumer table or view to the publisher's table or view as siblings. The consumer side is marked non-primary so that, when the publisher project is also ingested, its native emission becomes the primary sibling automatically.
- An `UpstreamLineage` aspect with a `COPY`-type edge from the publisher to the consumer, plus per-column `FineGrainedLineage` entries. Linked datasets mirror the publisher byte-identically, so column lineage is 1:1 by name and respects `convert_column_urns_to_lowercase`.

If the publisher project has not been ingested into DataHub, lineage edges still emit and the publisher entity appears as a placeholder until the publisher project is ingested.

Subscriptions in `STATE_STALE` or `STATE_INACTIVE` are still ingested. The state lands in custom properties so you can filter on it downstream.

**Configuration:**

```yaml
source:
  type: bigquery
  config:
    include_linked_datasets: true # Default false. Detect linked datasets and override their subtype.
    include_linked_dataset_lineage: true # Default true. Only takes effect when include_linked_datasets is true.
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

The BigQuery Sharing integration in particular has the following limitations:

- **Pub/Sub linked resources are not handled.** Only Analytics Hub subscriptions whose `resource_type` is `BIGQUERY_DATASET` are processed.
- **Lineage is skipped when `resourcemanager.projects.get` is denied on a publisher project.** The linked dataset is still ingested, but no Sibling or UpstreamLineage edges are emitted for that subscription. See the prerequisites section for details.
- **Subtype reclassification happens on the next run.** If a dataset was previously ingested as a regular `Dataset` and you later enable `include_linked_datasets`, the next ingestion run reclassifies it to `Linked Dataset` via standard UPSERT semantics. No manual migration is needed.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
