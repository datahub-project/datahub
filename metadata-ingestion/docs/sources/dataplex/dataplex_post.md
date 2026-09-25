### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution
The Google Cloud Knowledge Catalog (Dataplex) connector will overwrite metadata from other Google Cloud source connectors (BigQuery, GCS, etc.) if they extract the same entities. If you're running multiple Google Cloud connectors, be aware that the last connector to run will determine the final metadata state for overlapping entities.
:::

#### Platform Alignment

Datasets discovered use the same URNs as native connectors (e.g., `bigquery`, `gcs`). This means:

- **No Duplication**: Google Cloud Knowledge Catalog (Dataplex) and native BigQuery/GCS connectors can run together - entities discovered by both will merge
- **Native Containers**: BigQuery tables appear in their native dataset containers
- **Unified View**: Users see a single view of all datasets regardless of discovery method

#### Custom Properties

The connector adds the following custom properties to datasets:

| Property                        | Always Present | Description                                                                              |
| ------------------------------- | -------------- | ---------------------------------------------------------------------------------------- |
| `dataplex_ingested`             | Yes            | Marker indicating the dataset was ingested via Google Cloud Knowledge Catalog (Dataplex) |
| `dataplex_entry_id`             | Yes            | The entry identifier in Google Cloud Knowledge Catalog (Dataplex)                        |
| `dataplex_entry_group`          | Yes            | The entry group containing this entry                                                    |
| `dataplex_fully_qualified_name` | Yes            | The fully qualified name of the entry                                                    |
| `dataplex_entry_type`           | No             | The Google Cloud Knowledge Catalog (Dataplex) entry type (e.g. `bigquery-table`)         |
| `dataplex_parent_entry`         | No             | The parent entry name, if set                                                            |
| `dataplex_source_resource`      | No             | The source resource identifier from the entry source                                     |
| `dataplex_source_system`        | No             | The source system from the entry source                                                  |
| `dataplex_source_platform`      | No             | The source platform from the entry source                                                |
| `dataplex_aspect_<aspect_type>` | No             | One property per aspect attached to the entry, named after the aspect type               |

#### Filtering Configuration

Filter which datasets to ingest using regex patterns with allow/deny lists:

**Example:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    filter_config:
      entries:
        pattern:
          allow:
            - "production_.*" # Only production datasets
          deny:
            - ".*_test" # Exclude test datasets
            - ".*_temp" # Exclude temporary datasets
```

#### Lineage

When `include_lineage` is enabled and proper permissions are granted, the connector extracts **table-level lineage** using the Dataplex Lineage API. The connector automatically tracks lineage from these Google Cloud systems:

**Supported Systems:**

- **BigQuery**: DDL (CREATE TABLE, CREATE TABLE AS SELECT, views, materialized views) and DML (SELECT, INSERT, MERGE, UPDATE, DELETE) operations
- **Cloud Data Fusion**: Pipeline executions
- **Cloud Composer**: Workflow orchestration
- **Dataflow**: Streaming and batch jobs
- **Dataproc**: Apache Spark and Apache Hive jobs (including Dataproc Serverless)
- **Vertex AI**: Models, datasets, feature store views, and feature groups

:::note
Only **BigQuery** lineage has been thoroughly tested with this connector. Lineage from other systems may work but has not been validated.
:::

**Not Supported:**

- **Custom sources**: Only Google Cloud systems with automatic lineage tracking are supported
- **BigQuery Data Transfer Service**: Recurring loads are not automatically tracked

**Lineage Limitations:**

- Lineage data is retained for 30 days in Google Cloud Knowledge Catalog (Dataplex)
- Lineage may take up to 24 hours to appear after job completion
- Lineage is only available for entries with active lineage tracking enabled

For more details, see [Google Cloud Knowledge Catalog (Dataplex) Lineage Documentation](https://docs.cloud.google.com/dataplex/docs/about-data-lineage).

#### Column-level Lineage

Set `include_column_lineage: true` to additionally extract column-to-column
mappings. For each entry that has table-level lineage, the connector issues one
extra `search_links` call per batch of 20 columns per parent that returned
links, and attaches the result as `fineGrainedLineages` on the same
`upstreamLineage` aspect.

Column lineage requires both `include_lineage` and `include_schema`. If either
is off, column lineage is disabled with a warning rather than failing the
source.

Because this multiplies Data Lineage API read volume, lower
`max_workers_lineage` to slow the call rate down: the pool size is the binding
constraint on sustained throughput, while `lineage_max_calls_per_minute` is a
ceiling that only engages once the pool can outrun it.

Nested columns are matched on their simplified dotted names — which is how the
Data Lineage API addresses them — and emitted against the connector's own
`[version=2.0]` fieldPaths, so both ends of a fine-grained edge resolve to
fields that exist in `schemaMetadata`. A column the API names but the schema
does not contain is counted under `num_column_names_unmatched` and skipped.

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    include_lineage: true
    include_schema: true
    include_column_lineage: true
```

#### Lineage Upstreams Outside the Catalog

The Data Lineage API reports upstreams that are not Universal Catalog entries.
The connector resolves three such shapes:

| Reported FQN shape                    | Resolved to                                                                                             |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `gcs:{bucket}[/object/path]`          | A `gcs` dataset keyed on the bucket. Object paths are dropped so every producer lands on the same node. |
| `hive_metastore:{host}.{db}.{table}`  | The matching Dataproc Metastore table ingested in this run; see below for the fallbacks.                |
| `pubsub:subscription:{project}.{sub}` | The backing topic, resolved through the Pub/Sub Admin API. Opt in with `resolve_pubsub_subscriptions`.  |

Spark and Dataproc jobs report lineage as
`hive_metastore:` `` `<thrift-host>` ``.`{database}.{table}`, which carries no
project, location or service. The connector resolves that pair in order:

1. an exact (then case-insensitive) match against the Dataproc Metastore tables
   ingested in this run;
2. the `dpms_hive_metastore_service` fallback (`{project}.{location}.{service}`),
   which mints a Dataproc Metastore URN under the configured service;
3. with `include_hive_metastore_nodes` (default `true`), a lineage-only `hive`
   dataset named `{database}.{table}` — matching DataHub's Hive connector
   convention, so a later real Hive ingestion of the same metastore joins those
   URNs rather than duplicating them. A pair that exists under two metastore
   services is ambiguous and also lands here rather than being guessed;
4. otherwise the edge is skipped and counted under
   `num_hive_metastore_fqns_unresolved`.

Dataplex never reports a table's backing-storage hop as lineage, so
`include_storage_lineage: true` derives a bucket → table edge for Dataproc
Metastore tables from the entry's own `storage` aspect. It is off by default
because it adds an upstream that no previous run emitted.

#### Lineage-only Upstream Nodes

`searchAcrossLineage` — and therefore the UI's Lineage tab — only returns
entities that carry a `status` aspect. An upstream that exists purely because a
lineage edge points at it (a GCS bucket, an uncatalogued hive table, or a table
in a project outside the configured scope) is otherwise never materialized, so
the edge exists in the graph but the node is invisible.

Set `include_lineage_only_upstreams: true` to materialize a minimal entity for
those URNs. What gets written is decided per URN:

| State of the URN                    | What is written                                                  |
| ----------------------------------- | ---------------------------------------------------------------- |
| Emitted by this run's entries stage | `status` only — that stage owns the rest                         |
| Absent from DataHub                 | `status`, display name, subtype, and the missing container chain |
| Present and not deleted             | `status` only, so whoever owns its properties keeps them         |
| Soft-deleted                        | Nothing. A tombstone is never cleared                            |

Deciding this needs a DataHub graph connection, so without one nothing outside
the run's own entities is written. Note that `gcs` and `hive` URNs get no
special treatment: DataHub's own object-store and Hive connectors produce URNs
in those namespaces too, so they are checked like everything else.

#### Sticky Lineage

By default (`remove_stale_lineage: true`) each run overwrites `upstreamLineage`
with the edges the Data Lineage API currently reports. Because that API retains
links for a limited window, set `remove_stale_lineage: false` to merge new
edges with the previously persisted aspect instead: a freshly observed upstream
always wins over its persisted copy, edges the API no longer reports are
preserved, and lineage-only upstream entities are exempted from
stale-metadata removal. Merging reads the persisted aspect, so it needs the
DataHub graph client; a dry run without one emits the fresh state only.

Either way, entities that disappear from the source are still soft-deleted by
stateful ingestion.

#### Lineage Operation Nodes

Set `include_lineage_operations: true` to resolve each lineage link to the GCP
process that produced it and emit that process as a DataHub Query entity,
referenced from the edge so the UI renders an operation node on it.

This adds `batchSearchLinkProcesses`, `getProcess` and `listRuns` reads, all
through the same rate limiter; caching keeps it to one `getProcess` and one
single-row `listRuns` per distinct process per run. A link produced by several
processes collapses to the one with the latest end time, since an edge's query
pointer is single-valued. Use `lineage_operation_origin_types` to deny specific
origins (`BIGQUERY`, `DATAFLOW`, `COMPOSER`, `DATAPROC`, ...) when another
connector already owns query nodes for those tables — denied origins keep their
lineage edges and simply get no operation node.

The Data Lineage API stores only a BigQuery job id, never the statement, so the
query node carries a synthetic comment-style statement by default. Set
`include_lineage_operation_sql: true` to fetch the real SQL from the BigQuery
Jobs API instead; this needs `roles/bigquery.resourceViewer` on the compute
projects, because jobs are private to their creator by default, and the
`google-cloud-bigquery` package, which the `dataplex` extra does not install
(for example `pip install 'acryl-datahub[dataplex,bigquery]'`). Any failure
degrades back to the synthetic statement.

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    include_lineage: true
    include_lineage_operations: true
    # include_lineage_operation_sql: true
    lineage_operation_origin_types:
      deny:
        - "BIGQUERY" # BigQuery query nodes already come from the BigQuery connector
```

#### Configuration Options

**Metadata Extraction:**

- **`include_schema`** (default: `true`): Extract column metadata and types. Columns that carry structure — `REPEATED` mode, nested `fields`, or a hive-style complex type spelling such as `array<struct<...>>` — are expanded into nested `[version=2.0]` fieldPaths, the same representation the BigQuery connector emits, so the UI renders them as Array/Struct with expandable children.
- **`include_lineage`** (default: `true`): Extract table-level lineage (automatically retries transient errors)
- **`include_column_lineage`** (default: `false`): Extract column-to-column lineage; see [Column-level Lineage](#column-level-lineage)
- **`include_lineage_operations`** (default: `false`): Emit the producing GCP process as an operation node on each edge; see [Lineage Operation Nodes](#lineage-operation-nodes)
- **`include_lineage_only_upstreams`** (default: `false`): Materialize minimal entities for upstreams this run does not otherwise ingest; see [Lineage-only Upstream Nodes](#lineage-only-upstream-nodes)
- **`remove_stale_lineage`** (default: `true`): Whether lineage mirrors the live API state; see [Sticky Lineage](#sticky-lineage)
- **`include_storage_lineage`** (default: `false`): Derive a bucket → table edge from a Dataproc Metastore table's `storage` aspect
- **`resolve_pubsub_subscriptions`** (default: `false`): Resolve `pubsub:subscription:` upstreams to their backing topic
- **`include_hive_metastore_nodes`** (default: `true`): Emit unmatched `hive_metastore:` upstreams as lineage-only `hive` datasets
- **`dpms_hive_metastore_service`** (default: unset): `{project}.{location}.{service}` fallback for resolving `hive_metastore:` upstreams

#### Parallel Processing

Entry detail fetching and lineage lookups are parallelised using thread pools to significantly
reduce wall-clock ingestion time for large deployments.

**Entries stage** runs in three phases:

1. `list_entry_groups` + `list_entries` — sequential listing across all project × location pairs
   (fast; no parallelism needed)
2. `get_entry(ALL)` calls — parallel across a flat worker pool so entries are distributed evenly
   regardless of how they are spread across projects
3. Spanner entries via `search_entries` — sequential (already fully-fetched, nothing to parallelise)

**Lineage stage** dispatches one worker per entry to fetch `search_links` results across all
configured `lineage_locations`, so total API call time scales with
`max(entries / max_workers_lineage)` rather than `entries × lineage_locations`.

Two config fields control the thread pool sizes:

| Field                 | Default | Description                                      |
| --------------------- | ------- | ------------------------------------------------ |
| `max_workers_entries` | `10`    | Workers for `get_entry` calls (entries stage)    |
| `max_workers_lineage` | `10`    | Workers for `search_links` calls (lineage stage) |

Increase these values for large deployments, subject to your GCP API quota limits.

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Parallel processing (tune to your deployment size and API quota)
    max_workers_entries: 20 # default: 10
    max_workers_lineage: 40 # default: 20
```

**Lineage Retry and Rate Limit Settings** (optional):

- **`lineage_max_retries`** (default: `3`, range: `1-20`): Retry attempts for transient errors
- **`lineage_retry_backoff_multiplier`** (default: `1.0`, range: `0.1-30.0`): Backoff delay multiplier
- **`lineage_retry_max_wait_seconds`** (default: `65`, range: `2-600`): Upper cap on the backoff between retries. The default sits just above the Data Lineage API's 60-second quota window, so a retry can land in a fresh window instead of burning every attempt inside the same exhausted one.
- **`lineage_max_calls_per_minute`** (default: `1000`): Client-side ceiling on Data Lineage API reads, enforced across all lineage workers. Google documents the read quota as 1000 requests/minute/project/user/region; lower this when several pipelines share the same quota.
- **`glossary_lookup_max_calls_per_minute`** (default: `600`): The same ceiling for `lookupEntryLinks` reads during term-association resolution.

**Example Configuration:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    # Location for entries (Universal Catalog) - defaults to ["us", "eu", "asia", "global"]
    # Must be multi-region (us, eu, asia) for system entry groups like @bigquery
    entries_locations:
      - "us"

    # Metadata extraction settings
    include_schema: true # Enable schema metadata extraction (default: true)
    include_lineage: true # Enable lineage extraction with automatic retries

    # Lineage retry settings (optional, defaults shown)
    lineage_max_retries: 3 # Max retry attempts (range: 1-20)
    lineage_retry_backoff_multiplier: 1.0 # Exponential backoff multiplier (range: 0.1-30.0)
    lineage_retry_max_wait_seconds: 65 # Backoff cap, just past the 60s quota window
    lineage_max_calls_per_minute: 1000 # Client-side Data Lineage API read ceiling
```

**Configuration for Large Deployments:**

For deployments with thousands of entries, memory optimization is important. The connector uses batched emission to keep memory bounded:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Performance tuning
    batch_size: 1000 # Process and emit 1000 entries at a time to optimize memory usage
```

#### Business Glossary

When `include_glossaries` is enabled (default), the connector ingests all [Dataplex Business Glossaries](https://cloud.google.com/dataplex/docs/glossaries-overview) from the configured `glossary_locations` (default: `global`) and emits the full Glossary → Category → Term hierarchy as DataHub Glossary entities.

Each term is emitted as a `GlossaryTerm` with:

- `term_source: EXTERNAL` and a `source_url` linking directly to the term in the Dataplex console
- `custom_properties` carrying `project_id`, `location`, `glossary_id`, and `term_id`

When `include_glossary_term_associations` is enabled (opt-in, default: `false`), the connector additionally resolves term-to-asset links using the Dataplex `lookupEntryLinks` API and attaches the corresponding terms to each linked DataHub dataset. For each term, the API is called at the term's location to retrieve all linked assets (regardless of where those assets are located). This phase runs after entries are ingested, so only assets already discovered by the entries stage can be linked. It requires a role granting `resourcemanager.projects.get` (such as [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser)) on all configured projects. See the Permissions table in the [Prerequisites](#permissions) section above and the GCP [Resource Manager roles reference](https://cloud.google.com/iam/docs/understanding-roles#resource-manager-roles).

:::warning Term associations replace existing glossary terms

`glossaryTerms` has no server-side merge, so each run writes the complete term list for an
asset. Terms added in the DataHub UI, or applied by another connector to the same entity, are
replaced by whatever Dataplex reports. Column-level terms are unaffected.

To keep Dataplex's terms scoped to Dataplex and leave everything else intact, add the
[`set_attribution`](../../../../metadata-ingestion/docs/transformer/set_attribution.md)
transformer to your recipe:

```yaml
transformers:
  - type: "set_attribution"
    config:
      attribution_source: "urn:li:dataPlatform:dataplex"
```

With the default `patch_mode: false`, a run still replaces Dataplex's own terms — so unlinking
a term in Dataplex removes it from DataHub on the next run — while terms attributed to any
other source are left untouched.

:::

**Configuration:**

| Field                                | Default    | Description                                                                                                                                                                                                                |
| ------------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `include_glossaries`                 | `true`     | Ingest Dataplex Business Glossaries as `GlossaryNode`/`GlossaryTerm`                                                                                                                                                       |
| `include_glossary_term_associations` | `false`    | Attach glossary terms to linked datasets via `lookupEntryLinks`. Requires a role granting `resourcemanager.projects.get` such as [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser) (opt-in) |
| `glossary_locations`                 | `[global]` | GCP locations to scan for glossaries; most glossaries live in `global`                                                                                                                                                     |
| `max_workers_glossary`               | `10`       | Parallel workers for glossary ingestion and term-association lookups                                                                                                                                                       |

**Example:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Business Glossary ingestion (enabled by default)
    include_glossaries: true
    glossary_locations:
      - "global"

    # Term-to-asset associations (opt-in; requires roles/browser or another
    # role granting resourcemanager.projects.get on each configured project)
    # include_glossary_term_associations: true
# Scope term writes to Dataplex so terms curated in the UI are not replaced.
# See the warning above.
# transformers:
#   - type: "set_attribution"
#     config:
#       attribution_source: "urn:li:dataPlatform:dataplex"
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Entity Type Support

Dataplex entries map to DataHub **Dataset** entities (BigQuery tables and views, Cloud SQL for MySQL tables, Spanner tables and graphs, Bigtable tables, Pub/Sub topics, Vertex AI datasets, feature groups and feature online stores, Dataproc Metastore tables), DataHub **Container** entities (BigQuery datasets, Cloud SQL for MySQL instances and databases, Spanner instances and databases, Bigtable instances, Dataproc Metastore services and databases), or DataHub **MLModel** entities (Vertex AI model versions).

**Lineage extraction** applies to Dataset entities only. Containers and ML models have no lineage in Dataplex, so no lineage is emitted for them.

**Glossary term associations** apply to both Dataset and Container entities, so a term attached to a BigQuery dataset in Dataplex appears on the corresponding DataHub container. Only assets already discovered by the entries stage can be linked — a term pointing at an entry outside the configured projects, `entries_locations`, or `entries` pattern is reported under `term_links_unmatched` and skipped.

### Troubleshooting

#### Expected Knowledge Catalog (Dataplex) sync latency

Please be aware of the following documented delays when using this connector. These are standard Knowledge Catalog (Dataplex) behaviors and typically do not indicate an error:

- Metadata Sync: Updates to metadata or new entries can take up to 10 minutes to appear due to backend caching.
- Data Lineage: Lineage graphs are not real-time; updates typically take 30 minutes to 3 hours, but can take up to 24 hours to fully populate.
- Data Quality Results: Results from Auto Data Quality scans may have a slight processing delay before appearing in the UI.

If updates exceed these windows, check your [Cloud Logging](https://docs.cloud.google.com/dataplex/docs/logging) for specific job errors or permission issues.

#### Lineage Extraction Issues

**Automatic Retry Behavior:**

The connector automatically retries transient errors when extracting lineage:

- **Retried errors** (with exponential backoff): Timeouts (DeadlineExceeded), rate limiting (HTTP 429), service issues (HTTP 503, 500)
- **Non-retried errors** (logs warning and continues): Permission denied (HTTP 403), not found (HTTP 404), invalid argument (HTTP 400)

After exhausting retries, the connector logs a warning and continues processing other entries. You'll still get metadata even if lineage extraction fails for some entries.

**Common Issues:**

1. **Location scope**: Lineage API requests are scoped using each entry's own Dataplex location.
2. **Missing permissions**: Ensure service account has `roles/datalineage.viewer` role on all projects.
3. **No lineage data**: Some entries may not have lineage if they weren't created through supported systems (BigQuery DDL/DML, Cloud Data Fusion, etc.).
4. **Rate limiting**: If you encounter persistent rate limiting, increase `lineage_retry_backoff_multiplier` to add more delay between retries, or decrease `lineage_max_retries` if you prefer faster failure.

#### Others

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
