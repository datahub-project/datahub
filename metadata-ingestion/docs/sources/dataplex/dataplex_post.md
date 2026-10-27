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
        # Matched on the fully qualified name, bigquery:<project>.<dataset>.<table>.
        # A BigQuery entry's resource name ends in a long API path, not the table.
        fqn_pattern:
          allow:
            - "bigquery:my-gcp-project\\.production_.*" # Only datasets named production_*
          deny:
            - ".*_test$" # Exclude test tables
            - ".*_temp$" # Exclude temporary tables
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

The Data Lineage API names nested columns by simple dotted names. Downstream
columns are matched to the entry's own fieldPaths (`[version=2.0]` paths for
nested columns); a downstream column the schema does not contain is skipped
and counted under `num_column_names_unmatched`. Upstream columns are remapped
to their own fieldPaths only when the upstream is ingested in the same run;
otherwise the API's dotted name is emitted as is.

If a column lookup for an entry fails, that entry's `upstreamLineage` is not
emitted for the run, so the lineage stored by an earlier run is kept.

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

| Reported FQN shape                    | Resolved to                                                                                            |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `gcs:{bucket}.{path}`                 | A `gcs` dataset keyed on the bucket. Paths are dropped so every producer lands on the same node.       |
| `hive_metastore:{host}.{db}.{table}`  | The matching Dataproc Metastore table ingested in this run; see below for the fallbacks.               |
| `pubsub:subscription:{project}.{sub}` | The backing topic, resolved through the Pub/Sub Admin API. Opt in with `resolve_pubsub_subscriptions`. |

Spark and Dataproc jobs report lineage as
`hive_metastore:` `` `<thrift-host>` ``.`{database}.{table}`, which carries no
project, location or service. The connector resolves that pair in order:

1. an exact (then case-insensitive) match against the Dataproc Metastore tables
   ingested in this run. A pair that exists under more than one metastore
   service is not guessed: it is counted under
   `num_hive_metastore_fqns_ambiguous` and goes on to the next step;
2. the `dpms_hive_metastore_service` fallback (`{project}.{location}.{service}`):
   that service's table if this run ingested it, otherwise a Dataproc
   Metastore URN minted under the configured service;
3. with `include_hive_metastore_nodes` (default `true`), a `hive` dataset URN
   named `{database}.{table}` — matching DataHub's Hive connector convention, so
   a later real Hive ingestion of the same metastore joins those URNs rather
   than duplicating them. Only the edge is written; the node itself needs
   [`include_lineage_only_upstreams`](#lineage-only-upstream-nodes);
4. otherwise the edge is skipped and counted under
   `num_hive_metastore_fqns_unresolved`.

Dataplex never reports a table's backing-storage hop as lineage, so
`include_storage_lineage: true` derives a bucket → table edge for Dataproc
Metastore tables from the entry's own `storage` aspect. It is off by default
because the edge is derived from catalog metadata, not reported by the Data
Lineage API. When some lineage locations fail and no link from the rest resolves
to an upstream, the edge is withheld for that run and counted under
`num_storage_lineage_withheld`. A location that fails on every run withholds it
every time, so remove that location from `lineage_locations`. A lookup where
every location fails emits nothing.

#### Lineage-only Upstream Nodes

An upstream that exists only because a lineage edge points at it (a GCS bucket,
an uncatalogued hive table, or a table in a project outside the configured
scope) has no entity in DataHub, so the edge exists but the node does not show
in lineage views. Set `include_lineage_only_upstreams: true` to create a minimal
entity for it: `status`, display name and subtype.

- Only upstreams that do not exist in DataHub yet are written. An upstream that
  already exists, for example one another connector ingests, is left untouched.
- These entities are never tracked for stale-metadata removal, so this connector
  never soft-deletes them, even after the edge disappears.
- Checking which upstreams exist needs a DataHub graph connection (the
  `datahub-rest` sink or `datahub_api`). Without one, nothing is written and the
  report says why. A failed check writes nothing for that upstream, warns once
  and counts it under `num_upstream_node_checks_failed`.
- A BigQuery or Spanner upstream gets no subtype, because a table and a view (or
  a graph) share one URN shape.
- The display name is sent as a patch. If another connector creates the same
  entity between the check and the write, its other properties survive, but its
  `status` and subtype are replaced until that connector runs again.
- With the pipeline flag `auto_resolve_lineage_urns` enabled, no node is written
  for an upstream on a platform it reconciles, because it may later point the
  edge at another casing of the URN. These are counted under
  `num_upstream_nodes_casing_skipped`.

#### Incremental Lineage

By default, an entry that has edges this run gets its `upstreamLineage`
replaced with them, so an edge the Data Lineage API stops reporting is removed.
An entry with no edges this run is not written and keeps its stored edges. The
API keeps links for a limited time, so set `incremental_lineage: true` to send
lineage as a patch instead: new edges are added and edges the API no longer
reports are kept. Patches need a DataHub server that supports them.

#### Configuration Options

**Metadata Extraction:**

- **`include_schema`** (default: `true`): Extract column metadata and types. Columns that carry structure — `REPEATED` mode, nested `fields`, or a complex type spelling such as `array<struct<...>>` or `ARRAY<INT64>` (matched case-insensitively) — are expanded into nested `[version=2.0]` fieldPaths, the same representation the BigQuery connector emits, so the UI renders them as Array/Struct with expandable children. On BigQuery entries, nested field names are lowercased to match the BigQuery connector. Complex type spellings the Hive type parser cannot read, such as `ARRAY<STRING(MAX)>` or `STRUCT<a INT64>`, keep a single flat row.
- **`include_lineage`** (default: `true`): Extract table-level lineage (automatically retries transient errors)
- **`include_column_lineage`** (default: `false`): Extract column-to-column lineage; see [Column-level Lineage](#column-level-lineage)
- **`include_lineage_only_upstreams`** (default: `false`): Create minimal entities for upstreams that do not exist in DataHub; see [Lineage-only Upstream Nodes](#lineage-only-upstream-nodes)
- **`incremental_lineage`** (default: `false`): Add new lineage edges without removing old ones; see [Incremental Lineage](#incremental-lineage)
- **`include_storage_lineage`** (default: `false`): Derive a bucket → table edge from a Dataproc Metastore table's `storage` aspect
- **`resolve_pubsub_subscriptions`** (default: `false`): Resolve `pubsub:subscription:` upstreams to their backing topic
- **`include_hive_metastore_nodes`** (default: `true`): Point `hive_metastore:` upstreams that neither this run's tables nor `dpms_hive_metastore_service` resolve at a `hive` dataset URN named `{database}.{table}`; creating the node needs `include_lineage_only_upstreams`
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
Each lineage location is paced separately at `lineage_max_calls_per_minute`, so more
workers stop helping once every location runs at that rate.

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
    max_workers_lineage: 40 # default: 10
```

**Lineage Retry and Rate Limit Settings** (optional):

- **`lineage_max_retries`** (default: `3`, range: `1-20`): Retry attempts for transient errors
- **`lineage_retry_backoff_multiplier`** (default: `1.0`, range: `0.1-30.0`): Backoff delay multiplier
- **`lineage_retry_max_wait_seconds`** (default: `10`, range: `2-600`): Upper cap on the backoff between retries. With the default retry settings the backoff never reaches it; it matters only after raising `lineage_max_retries` or `lineage_retry_backoff_multiplier`. Together with more retries or a larger multiplier, set it to about `65`, just past the Data Lineage API's 60-second quota window, so a retry after a 429 can land in a fresh window.
- **`lineage_max_calls_per_minute`** (default: `1000`): Client-side ceiling on Data Lineage API `search_links` requests per lineage location, shared by all lineage workers and all `project_ids`, and charged once per attempt, including retries. Result pages after the first are not counted. Google's read quota is 1000 requests per minute per project in a multi-region and 2000 in a single region; lower this when several pipelines share a project.
- **`glossary_lookup_max_calls_per_minute`** (default: `600`): One ceiling for all `lookupEntryLinks` reads during term-association resolution. The default is a conservative choice because Dataplex publishes no separate quota for `lookupEntryLinks`.

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
    lineage_retry_max_wait_seconds: 10 # Backoff cap; with more retries, ~65 outlasts the 60s quota window
    lineage_max_calls_per_minute: 1000 # Data Lineage API read ceiling per location
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

#### Probe support

`datahub recipe probe` lists what the catalog holds and judges it against your recipe's filters before you run ingestion:

```bash
datahub recipe probe run entry_groups --recipe recipe.yml --project my-project
datahub recipe probe run entries --recipe recipe.yml --project my-project \
  --entry_group projects/my-project/locations/us/entryGroups/my-group
datahub recipe probe filter --recipe recipe.yml --kind Entry \
  --parent my-project --parent projects/my-project/locations/us/entryGroups/my-group \
  --name projects/my-project/locations/us/entryGroups/my-group/entries/my-entry
```

The `filter_config` patterns match full resource names exactly as the API returns them, and a pattern must match from the start of the name. Copy names from the `entry_groups` and `entries` output instead of writing them by hand, because the project segment may be a project number. An entry is ingested only when both `filter_config.entries.pattern` (on its name) and `filter_config.entries.fqn_pattern` (on its fully-qualified name) include it. `--kind Entry` judges both when it is given an `entries` listing with `--from-run`, which carries each FQN; for bare names, check the FQNs with `--kind EntryFqn`. The `export` and `read_export` extraction methods do not apply `filter_config.entry_groups.pattern`. In the default `api` method, Spanner entries are read through a catalog search that also bypasses `filter_config.entry_groups.pattern`, so only the entry and project filters decide them.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Entity Type Support

Dataplex entries map to either DataHub **Dataset** entities (BigQuery tables and views, Cloud SQL for MySQL tables, Spanner tables and graphs, Bigtable tables, Pub/Sub topics, Vertex AI datasets, Dataproc Metastore tables) or DataHub **Container** entities (BigQuery datasets, Cloud SQL for MySQL instances and databases, Spanner instances and databases, Bigtable instances, Dataproc Metastore services and databases).

**Lineage extraction** applies to Dataset entities only. Containers have no lineage in Dataplex, so no lineage is emitted for them.

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
4. **Rate limiting**: If you get persistent HTTP 429 errors, lower `lineage_max_calls_per_minute` (or `glossary_lookup_max_calls_per_minute` for `lookupEntryLinks` errors) so the connector sends fewer requests. You can also increase `lineage_retry_backoff_multiplier` to add more delay between retries, or decrease `lineage_max_retries` if you prefer faster failure.

#### Others

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
