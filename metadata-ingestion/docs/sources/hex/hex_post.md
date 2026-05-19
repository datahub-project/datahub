### Capabilities

#### Upstream Lineage

Two tiers are tried in order:

1. **`queriedTables` API (Hex ENTERPRISE tier).** The connector calls `/v1/projects/{id}/queriedTables`, which returns Hex's own list of fully qualified tables queried by each project. A `403` falls back to tier 2.
2. **SQL parsing (all tiers).** SQL cells are parsed with [`sqlglot`](https://github.com/tobymao/sqlglot) using each connection's dialect. Unqualified `FROM table` refs are resolved using the connection's default database/schema from `/v1/data-connections`, overridable via `connection_platform_map`.

##### Connection Platform Resolution

Hex's `/v1/data-connections` endpoint returns a `type` field that the connector maps to a DataHub platform via [`CONNECTION_TYPE_TO_DATAHUB_PLATFORM`](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/hex/constants.py). Default database/schema qualifiers come from the same response.

Configure `connection_platform_map` (keyed by Hex `dataConnectionId` UUID) when:

1. The warehouse was ingested under a `platform_instance` — set the matching value so URNs collide.
2. The connection is deleted, permission-gapped, or a custom type — set `platform` explicitly so its cells aren't skipped.

Example:

```yaml
connection_platform_map:
  "8f3a1c2d-4b5e-6789-abcd-ef0123456789":
    platform: snowflake
    platform_instance: prod_snowflake
    default_database: ANALYTICS
    default_schema: PUBLIC
  "1a2b3c4d-5e6f-7890-abcd-1234567890ab":
    platform: bigquery
    default_database: my-gcp-project
```

#### Incremental Ingestion

Enable by configuring `stateful_ingestion`. On subsequent runs:

- **Unchanged projects** skip cell, lineage, and context-document fetches. Only the latest run record is refreshed (when `include_run_history` is enabled).
- **Changed projects** are fully re-processed.
- **Projects deleted in Hex** are soft-deleted in DataHub.

`max_projects` caps projects per run. With `stateful_ingestion` enabled, projects beyond the limit are treated as stale and soft-deleted — only set it if that is the intended behavior.

#### Context Documents

When `include_context_documents` is enabled (default), the connector emits a DataHub Document per Project and per Component containing SQL sources, visualization metadata, and notebook documentation.

#### Run History

When `include_run_history` is enabled (default), the most recent scheduled run is emitted as an Operation aspect, and `last_run_status` / `last_run_elapsed_seconds` are written to the project's custom properties — `ERRORED` runs surface there so operators can see failures. Only `COMPLETED` runs additionally update `DashboardInfo.lastRefreshed` via a targeted PATCH, so projects with sustained failures keep their last known-good refresh time as a freshness signal.

#### Usage Statistics

Each Project and Component emits an all-time `viewsCount` and a rolling 7-day window with `lastViewedAt`. Hex counts app views only when the published app is accessed — unpublished drafts have no view counts, so usage statistics are only emitted for **published** Projects and Components.

### Limitations

1. **`queriedTables` is ENTERPRISE-only.** Workspaces on lower Hex tiers fall back to SQL parsing for lineage.
2. **Non-SQL query paths produce no lineage.** SQL parsing cannot recover table references from `hextoolkit` Python cells, dynamic SQL built from variables, or parameterized table names — the resulting projects will be missing those upstreams.
3. **Context documents are not a complete mirror of the Hex notebook.** Only a subset of cell types is captured, so the rendered document will not match the source notebook exactly.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first, then review ingestion logs for source-specific errors.

#### Missing Upstream Lineage

The source report lists every skipped cell with its `dataConnectionId` and a reason (`missing_connection_id` or `unresolved_platform`). For each unresolved connection, add an entry under `connection_platform_map` and re-run. Cells with no `dataConnectionId` are non-SQL cells or cells without a Hex connection assigned — these cannot be recovered.

#### Column Lineage Looks Sparse

On ENTERPRISE, the report exposes `enterprise_cells_with_mismatch` and `enterprise_sample_mismatched_cells` — SQL cells whose parsed table URN did not match the `queriedTables` result. Adjusting `default_database` / `default_schema` in `connection_platform_map` resolves most cases.
