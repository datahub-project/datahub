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

#### BigQuery Sharing (Linked Datasets)

A linked dataset is a read-only pointer into a dataset published by another project through BigQuery Sharing (formerly Analytics Hub). Subscribing is a console action rather than a query, so lineage derived from query history never observes it. Without this support the tables appear with correct schemas and no upstream.

Set `include_linked_datasets: true` to enable this. It is off by default because it changes the subtype of containers already in your catalogue and points lineage at a project you may not ingest. See the caveats below.

Once enabled it needs no additional permission: linked datasets are detected from the dataset listing the connector already fetches, so detection itself costs nothing.

**On the dataset container**, the subtype becomes `Linked Dataset` rather than `Dataset`, and three custom properties are added. The source is split into project and dataset because the publisher sits in a different project from the `project_id` already on the container:

| Property            | Example             | Meaning                                                                                                     |
| ------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------- |
| `source_project_id` | `publisher-project` | The project holding the dataset this one mirrors. BigQuery reports it as a number; it is resolved to its ID |
| `source_dataset_id` | `shared_data`       | The dataset this one points at, inside that project                                                         |
| `link_state`        | `LINKED`            | BigQuery's own view of whether the link is live                                                             |

A dataset already catalogued as `Dataset` is reclassified to `Linked Dataset` on the first run after you enable this. No migration or manual step is needed, and turning the flag back off returns it to `Dataset` on the following run.

**On each table and view**, a `COPY` upstream is emitted to the corresponding object in the source dataset, with a 1:1 column mapping. The column mapping is an identity mapping rather than an inference: a share copies nothing and renames nothing. The edge is written on the linked dataset's own tables only, so two recipes reading different consumers of one publisher never overwrite each other.

If `link_state` is present and reads anything other than `LINKED`, BigQuery is reporting the link as no longer live, and no lineage is emitted for that dataset. If BigQuery reports no `link_state` at all, lineage is still emitted, since an absent field is not evidence the link is dead. Either way the dataset and its properties are ingested, so the state is visible on the container.

Lineage requires `include_table_lineage`, which is on by default. With it off, linked datasets are still catalogued with their source reference and link state, but no `COPY` edge is emitted.

Set `extract_subscriptions_from_analytics_hub: true` to additionally record the listing and subscription state. That reads the BigQuery Sharing API and needs `analyticshub.subscriptions.list`; see Prerequisites.

| Property             | Example               | Meaning                                                                  |
| -------------------- | --------------------- | ------------------------------------------------------------------------ |
| `listing_id`         | `shared_data_listing` | The listing subscribed to                                                |
| `subscription_state` | `ACTIVE`              | `STALE` means the publisher has changed the listing since you subscribed |

With `include_linked_datasets` unset, a linked dataset is catalogued as an ordinary `Dataset` with no source reference and no lineage. The connector behaves exactly as it did before this feature existed.

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

#### Linked Dataset Caveats

Lineage from a linked dataset points at the publisher's project, which introduces three conditions worth knowing:

- **The publisher's project must be resolvable.** BigQuery reports the source project as a number, and DataHub URNs use project IDs. If the ingestion account can already see that project in BigQuery the number resolves at no cost; otherwise it needs `resourcemanager.projects.get` there. With neither, no lineage is emitted for datasets shared from that publisher, and the ingestion report names the project it could not resolve.
- **Ingest the publisher's project as well, because column-level lineage depends on it.** Lineage points at the source table's URN. If that project is in no recipe, DataHub knows only the URN: the upstream node shows its full `project.dataset.table` name rather than the table name, carries no subtype, and has no columns. The column-level edges are still recorded, but there is nothing on the publisher's side to draw them against, so **they do not render**. Ingesting the publisher's project resolves all of this on the next run. In a share between two organisations that is not possible, and table-level lineage is as far as it goes.
- **Use the same `platform_instance` and `env` for both.** The upstream URN is built with the settings of the recipe reading the linked dataset. If the publisher is ingested by a separate recipe with different settings, the two URNs will not match.

BigQuery returns no DDL through a share, so a view inside a linked dataset arrives with an empty view definition: `viewProperties` carries no logic and there is no SQL to parse view lineage from. The `COPY` edge to the source view is emitted regardless, and is the only lineage such a view gets.

Table snapshots inside a linked dataset are catalogued with their schema but receive no `COPY` upstream. A share does carry snapshots, but BigQuery does not report which table a shared snapshot was taken from, so there is nothing to point the edge at. Tables and views are unaffected.

With `extract_subscriptions_from_analytics_hub` enabled, only subscriptions to BigQuery datasets are read. A project's Pub/Sub subscriptions sit alongside them in the same API and are skipped without comment.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
