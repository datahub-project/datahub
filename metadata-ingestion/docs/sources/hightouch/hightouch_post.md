### Capabilities

This connector extracts the following metadata from Hightouch:

- **Sources** — Database and warehouse connections (as DataHub Dataset references)
- **Models** — SQL queries and transformations (optionally as DataHub Datasets)
- **Syncs** — Data pipelines from models to destinations (as DataHub DataJobs)
- **Destinations** — Target systems like Salesforce, HubSpot, etc. (as DataHub Dataset references)
- **Sync Runs** — Execution history with detailed statistics (as DataHub DataProcessInstances)
- **Lineage** — Complete data flow from sources through models and syncs to destinations
- **Column-level Lineage** — Field mappings between source and destination systems

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
dependencies and creating column-level lineage. Column casing is normalized to match upstream
table schemas (e.g., Snowflake lowercasing). For best results:

- Ensure upstream source tables are already ingested into DataHub
- Configure `sources_to_platform_instance` for accurate URN generation
- Enable a DataHub graph connection for schema resolution

#### Sibling Relationships

For table-type models and simple raw_sql models, you can establish sibling relationships between
the Hightouch model and its upstream source table:

```yaml
include_sibling_relationships: true # default: true
```

When enabled, the Hightouch model is marked as the **primary** sibling and the upstream source
table is linked as a **secondary** sibling. Sibling aspects are only emitted on the upstream
table if it already exists in DataHub, preventing the creation of "ghost" entities. Requires
`sources_to_platform_instance` configuration for proper URN matching.

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

destinations_to_platform_instance:
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

Control which syncs and models are ingested using regex patterns:

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

#### Supported Source and Destination Types

This connector supports 30+ data source types and 50+ destination types. Platform detection
works automatically for major sources including Snowflake, BigQuery, Redshift, Databricks,
PostgreSQL, MySQL, SQL Server, and common SaaS destinations. For sources or destinations not
automatically recognized, configure the mapping manually using `sources_to_platform_instance`
and `destinations_to_platform_instance`. For the complete list of supported integrations, refer
to the [Hightouch documentation](https://hightouch.com/docs/destinations/overview/).

### Limitations

- **User Ownership**: The Hightouch API does not expose creator/owner information for syncs, so
  ownership metadata is not extracted.
- **API Rate Limits**: The Hightouch API has rate limits. The connector implements retry logic
  with exponential backoff, but very large workspaces with thousands of syncs may need to be
  ingested in batches.
- **Platform Detection**: Automatic platform mapping works for major databases and SaaS tools.
  Custom or less common source/destination types may need manual platform mapping via
  `sources_to_platform_instance` or `destinations_to_platform_instance`.
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
   `destinations_to_platform_instance`
2. Check that `emit_models_as_datasets` is set to your preferred mode
3. Ensure source and destination datasets already exist in DataHub (or ingest them separately)

#### Rate Limiting

If you see `429 Too Many Requests`, the connector will automatically retry with backoff. For
large workspaces, consider using `sync_patterns` / `model_patterns` to reduce scope, or reducing
`max_sync_runs_per_sync` to limit API calls.
