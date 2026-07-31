### Capabilities

#### Upstream Lineage

Lineage is tiered, with both tiers opt-out via `include_lineage: false`:

- **Tier 1 — `queriedTables` (Hex Enterprise workspaces only, opt-in via `use_queried_tables_lineage: true`)**: Hex's own runtime-proven table list for **published** projects and components, served by `/v1/projects/{id}/queriedTables`. Unpublished entities always fall back to Tier 2 since `queriedTables` is only populated for published runs. A `403` (non-Enterprise Hex workspace) falls back to Tier 2 for everything and emits a warning.
- **Tier 2 — SQL parsing via [`sqlglot`](https://github.com/tobymao/sqlglot) (all workspaces, default)**: each cell is parsed with its connection's dialect.

Both tiers resolve warehouse URNs via `/v1/data-connections` (platform + default database/schema), overridable per-connection via `connection_platform_map`. For projects that import components, native project SQL is separated from inlined component SQL via the export API so component lineage isn't attributed twice. Cells whose `dataConnectionId` cannot be resolved are skipped with a structured warning — see [Missing Upstream Lineage](#missing-upstream-lineage) for triage.

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

#### Migration from `query_fetcher`

Earlier versions of this connector derived lineage by querying DataHub for prior Hex-emitted query metadata (`query_fetcher.py`). That path has been removed: lineage now comes from SQL parsing of cells by default, or from Hex's `queriedTables` API when `use_queried_tables_lineage: true` is set on a Hex Enterprise workspace.

The following config fields fed only the old path and are now removed — drop them from your recipe (the connector will emit a warning if they are still present):

- `lineage_start_time`
- `lineage_end_time`
- `datahub_page_size`

#### Migration: Components are now Charts

Components were previously emitted as **Dashboard** entities (subtype `Component`); they are now **Chart** entities, linked from their Project's `DashboardInfo.charts`. This changes their URN entity type, so any saved views, glossary/tag/ownership assignments, and policies that targeted the old Dashboard-typed Component URNs are lost and must be manually reapplied to the new Chart URNs.

Legacy Dashboard-typed Components left over from the old version are soft-deleted by stale-entity removal when `stateful_ingestion` was enabled on the old run. Because every Component changes URN type, a component-heavy workspace can exceed the stale-removal fail-safe (`fail_safe_threshold`, default 75%); if that happens, raise the threshold or perform a one-time bulk cleanup via the DataHub UI or CLI.

#### Stale Entity Removal

Enable by configuring `stateful_ingestion`. Projects deleted in Hex are soft-deleted in DataHub on the next run.

`max_projects` caps projects per run. With `stateful_ingestion` enabled, projects beyond the limit are treated as stale and soft-deleted — only set it if that is the intended behavior.

#### Context Documents

Opt-in via `include_context_documents: true`. When enabled, the connector emits a DataHub Document per Project and per Component containing SQL sources, visualization metadata, and notebook documentation.

#### Run History

When `include_run_history` is enabled (default), the most recent scheduled run is emitted as an Operation aspect, and `last_run_status` / `last_run_elapsed_seconds` are written to the project's custom properties — `ERRORED` runs surface there so operators can see failures. Only `COMPLETED` runs additionally update `DashboardInfo.lastRefreshed` via a targeted PATCH, so projects with sustained failures keep their last known-good refresh time as a freshness signal.

#### Usage Statistics

Each Project and Component emits an all-time `viewsCount` and a rolling 7-day window with `lastViewedAt`. Hex counts app views only when the published app is accessed — unpublished drafts have no view counts, so usage statistics are only emitted for **published** Projects and Components.

### Limitations

1. **`queriedTables` requires a Hex Enterprise workspace and opt-in.** Defaults to SQL parsing; enable `use_queried_tables_lineage` on Hex Enterprise workspaces to use Hex's API as the primary source.
2. **Non-SQL query paths produce no lineage.** SQL parsing cannot recover table references from `hextoolkit` Python cells, dynamic SQL built from variables, or parameterized table names — the resulting projects will be missing those upstreams.
3. **Context documents are not a complete mirror of the Hex notebook.** Only a subset of cell types is captured, so the rendered document will not match the source notebook exactly.
4. **Upstream lineage may be missing or mismatched when Hex's `/v1/data-connections` metadata is incomplete or uses an unrecognized `connectionDetails` shape.** Without `default_database` / `default_schema`, neither SQL parsing nor `queriedTables` can assemble fully-qualified URNs; without the right `platform_instance`, URNs won't align with the warehouse ingestion. Set the affected `dataConnectionId` under `connection_platform_map` with the correct `platform_instance` / `default_database` / `default_schema`, or report the new connection shape to the DataHub team so the parser can be updated.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first, then review ingestion logs for source-specific errors.

#### Missing Upstream Lineage

The source report lists every skipped cell with its `dataConnectionId` and a reason (`missing_connection_id` or `unresolved_platform`). For each unresolved connection, add an entry under `connection_platform_map` and re-run. Cells with no `dataConnectionId` are non-SQL cells or cells without a Hex connection assigned — these cannot be recovered.

#### Column Lineage Looks Sparse

When `use_queried_tables_lineage` is enabled on a Hex Enterprise workspace, the report exposes `enterprise_cells_with_mismatch` and `enterprise_sample_mismatched_cells` — SQL cells whose parsed table URN did not match the `queriedTables` result. Adjusting `default_database` / `default_schema` in `connection_platform_map` resolves most cases.
