### Capabilities

This connector extracts the following metadata from Hightouch:

- **Workspaces** — One container per Hightouch workspace, holding its models and syncs
- **Sources** — Database and warehouse connections (as DataHub Dataset references)
- **Models** — SQL queries and transformations (optionally as DataHub Datasets)
- **Syncs** — One DataFlow per workspace, with a DataJob per sync from its model to its destination
- **Destinations** — Target systems like Salesforce, HubSpot, etc. (as DataHub Dataset references)
- **Sync Runs** — Execution history with detailed statistics (as DataHub DataProcessInstances)
- **Lineage** — Complete data flow from sources through models and syncs to destinations
- **Column-level Lineage** — Field mappings between source and destination systems
- **Ownership** — The user who created a model or sync, as its technical owner
- **Tags** — Model and sync tags, as `key:value` tags
- **Event Contracts** — Event schema definitions emitted as Datasets, each with a DataHub DataContract housing a schema assertion, plus lineage from the contract's Event Sources

#### Model Ingestion Options

You can configure how Hightouch models are represented in DataHub:

- **Option 1 (Recommended)**: Set `emit_models_as_datasets: true`

  - Models appear as separate Dataset entities in DataHub with platform "hightouch"
  - Lineage flows: Source Tables → Hightouch Models → Syncs → Destination Tables
  - Provides visibility into intermediate transformations

- **Option 2**: Set `emit_models_as_datasets: false`
  - Models are not created as separate entities
  - Lineage flows directly: Source Tables → Syncs → Destination Tables
  - Simpler lineage graph, but less visibility into transformations

#### Lineage

The connector extracts comprehensive lineage:

1. **Coarse-grained Lineage**: Table-to-table relationships showing data flow
2. **Fine-grained Lineage**: Column-to-column mappings extracted from sync field mappings
3. **Multi-hop Lineage**: Complete path from source databases through models to destinations

SQL parsing is supported for `raw_sql` models, automatically extracting upstream table
dependencies and creating column-level lineage. The model's schema is the query's output columns.
When the upstream tables are already in DataHub, their names are matched with the casing they
were ingested with. Column casing is normalized to match upstream
table schemas (e.g., Snowflake lowercasing). For best results:

- Ensure upstream source tables are already ingested into DataHub
- Configure `sources_to_platform_instance` for accurate URN generation
- Enable a DataHub graph connection for schema resolution

#### Sibling Relationships

For table models, which expose a warehouse table as is, you can establish sibling relationships
between the Hightouch model and that table. Raw SQL models get lineage to the tables they read but
no sibling, because a query over a table is a different asset from the table. dbt models also get
lineage but no sibling, because the dbt connector already makes its dbt node the table's primary
sibling:

```yaml
include_sibling_relationships: true # default: true
```

When enabled, the Hightouch model is marked as the **primary** sibling and the upstream source
table is linked as a **secondary** sibling. Sibling aspects are only emitted on the upstream
table if it already exists in DataHub, preventing the creation of "ghost" entities. Requires
`sources_to_platform_instance` configuration for proper URN matching.

#### Event Contracts

Hightouch [Event Contracts](https://hightouch.com/docs/events/contracts) define the schemas used to
validate incoming events. Event Contracts are not available on every Hightouch plan, so they are
off by default. With `include_contracts: true`, the connector ingests them as:

- one **Dataset** per event, whose schema is the event's JSON Schema and whose custom properties
  capture the enforcement rules (`on_schema_violation`, `on_undeclared_fields`, `on_undeclared_schema`),
- a **DataContract** on each event dataset, housing a schema assertion derived from the JSON Schema,
- **lineage** from the contract's Event Sources (emitted as datasets) to each event dataset.

```yaml
include_contracts: true # default: false
contract_patterns:
  allow:
    - ".*"
```

The Hightouch API does not expose contract validation history, so the assertions are emitted as
definitions only (no pass/fail run results).

#### Platform Instance Configuration

If you have multiple instances of source or destination systems, configure platform instances to
generate correct lineage edges:

```yaml
sources_to_platform_instance:
  # Key is the Hightouch source ID (visible in Hightouch URL)
  "12345":
    platform: "snowflake"
    platform_instance: "prod-snowflake"
    env: "PROD"
    database: "analytics"
    # Lowercase upstream table names to match the source connector's URNs.
    # Defaults to true for Snowflake and false otherwise.
    convert_urns_to_lowercase: true

destination_to_platform_instance:
  # Key is the Hightouch destination ID
  "dest_123":
    platform: "salesforce"
    platform_instance: "prod-salesforce"
    env: "PROD"
```

To find source and destination IDs, navigate to the resource in the Hightouch UI and check the
URL (`https://app.hightouch.com/[workspace]/sources/[source_id]`), or query the Hightouch API:

```bash
curl -H "Authorization: Bearer YOUR_API_KEY" https://api.hightouch.com/api/v1/sources
```

#### Filtering

Control which syncs and models are ingested using regex patterns. `sync_patterns` match each
sync's slug, and `model_patterns` match each model's display name:

```yaml
sync_patterns:
  allow:
    - "prod-.*"
  deny:
    - "test-.*"
    - ".*-staging"

model_patterns:
  allow:
    - "customer-.*"
  deny:
    - ".*-draft"
```

#### Sync Run History

Limit the number of historical sync runs ingested:

```yaml
include_sync_runs: true
max_sync_runs_per_sync: 10 # last 10 runs per sync
```

Set `include_sync_runs: false` to skip sync run history entirely.
A run that has not finished (queued, querying, processing or reporting) is recorded with its
start only.

#### Supported Source and Destination Types

This connector supports 30+ data source types and 50+ destination types. Platform detection
works automatically for major sources including Snowflake, BigQuery, Redshift, Databricks,
PostgreSQL, MySQL, SQL Server, and common SaaS destinations. For sources or destinations not
automatically recognized, configure the mapping manually using `sources_to_platform_instance`
and `destination_to_platform_instance`. For the complete list of supported integrations, refer
to the [Hightouch documentation](https://hightouch.com/docs/destinations/overview/).

### Limitations

- **Ownership**: The creator of a model or sync becomes its technical owner, with their email
  address as the DataHub user id. Set `extract_owners: false` if your DataHub users are keyed
  differently.
- **Destination lineage**: Lineage to a destination is carried by the sync's DataJob. Nothing is
  written onto destination datasets, so their own connector's lineage is left as it is. A sync
  whose configuration names no destination object the connector recognises has no outlet, and is
  listed under `unresolved_destinations` in the report.
- **Model schemas**: The API returns no column list for a model. A table or dbt model takes its
  schema from the table it reads, when that table is already in DataHub, and a raw SQL model
  from its query's output columns. Otherwise the schema is the union of the columns its syncs
  reference, without types.
- **API Rate Limits**: The Hightouch API has rate limits. The connector implements retry logic
  with exponential backoff, but very large workspaces with thousands of syncs may need to be
  ingested in batches.
- **Platform Detection**: Automatic platform mapping works for major databases and SaaS tools.
  Custom or less common source/destination types may need manual platform mapping via
  `sources_to_platform_instance` or `destination_to_platform_instance`.
- **Complex SQL Queries**: SQL parsing works best for simple queries. Complex multi-table JOINs,
  CTEs, and subqueries may not produce complete column-level lineage.

### Troubleshooting

If ingestion fails, validate your API key, permissions, and connectivity first. Then review
ingestion logs for source-specific errors and adjust configuration accordingly.

#### Authentication Errors

If you see `401 Unauthorized`, verify your API key is correct and has not expired. Generate a
new key from **Settings** → **API Keys** in the Hightouch UI if needed.

#### Missing Lineage

If lineage is not appearing correctly:

1. Verify source and destination IDs are configured in `sources_to_platform_instance` /
   `destination_to_platform_instance`
2. Check that `emit_models_as_datasets` is set to your preferred mode
3. Ensure source and destination datasets already exist in DataHub (or ingest them separately)

#### Failed Runs and Stale Metadata

A sync or model that fails to process, Event Contracts that cannot be read (other than an API
key without access to them), an entity the API returns in an unexpected shape, or a list cut off
at the pagination limit fails the run. That keeps stale-metadata removal from soft-deleting
entities that still exist in Hightouch but were missing from the run. A single bad entity
therefore marks the whole run as failed on purpose, even though everything else was ingested;
the report names the entity.

A sync whose model could not be emitted reads directly from the model's warehouse tables instead,
so its lineage never points at a model that does not exist.

#### Rate Limiting

If you see `429 Too Many Requests`, the connector will automatically retry with backoff. For
large workspaces, consider using `sync_patterns` / `model_patterns` to reduce scope, or reducing
`max_sync_runs_per_sync` to limit API calls.
