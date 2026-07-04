### Capabilities

#### Container hierarchy

By default the container hierarchy is **Project → Space**. Set `include_organization_container: true` to wrap everything under a top-level Organization container — useful when you have multiple Lightdash organisations represented in the same DataHub instance.

#### Lineage to the warehouse

For each saved chart the connector resolves the Lightdash `Explore` it depends on and emits column-level lineage via the chart's `inputFields` aspect. Each Lightdash field that maps to a warehouse column is paired with a `schemaFieldUrn` of the form `urn:li:schemaField:(<dataset_urn>,<column>)`, where `<dataset_urn>` is the warehouse table that backs the explore.

The upstream warehouse platform is inferred from the Lightdash project's `warehouseConnection.type`. If you need to override the auto-detected platform name — for example when your DataHub instance uses a custom platform name for ClickHouse or Snowflake — set `warehouse_platform` in the source config. Use `warehouse_platform_instance` to attach a DataHub platform-instance qualifier to the upstream URNs.

#### Joined tables

When a Lightdash explore joins multiple warehouse tables, metrics that reference a column on a joined table will lineage to that joined table rather than the base table. This is resolved automatically from the explore's `tables` map (which Lightdash populates with one entry per joined table).

#### Lightdash field kinds

The connector covers all five Lightdash field kinds:

- `dimensions` and `metrics` — resolved against the explore's `tables[].dimensions` / `tables[].metrics`.
- `additionalMetrics` — chart-local metrics. Resolved from the `${TABLE}.col` reference in the metric's SQL.
- `customDimensions` (bin) — resolved via the bucketed `dimensionId` back to the underlying explore dimension.
- `customDimensions` (sql) — resolved by parsing `${TABLE}.col` references out of the SQL fragment.
- `tableCalculations` — see below.

#### Table calculations

Lightdash `tableCalculations` are post-query expressions over the result set; they have no upstream warehouse column. The connector counts them via `report.table_calculations_skipped` so you can audit how many fields were intentionally not lineaged, and does not emit them in the chart's `inputFields`.

### Limitations

- The connector calls `GET /api/v1/saved/{chart_uuid}`, `GET /api/v1/dashboards/{dashboard_uuid}`, and `GET /api/v1/projects/{id}/explores/{table}` once per chart, dashboard, and explore. Lightdash does not expose a batch endpoint today, so very large tenants (thousands of charts) will see proportionally longer ingestion runs. Explores are cached per `(project, table)` pair to avoid redundant calls.
- The Personal Access Token is scoped to the user that issued it; Lightdash does not currently provide a service-account model.
- Lightdash does not expose native tags or per-user usage statistics via its REST API today, so the connector does not emit `globalTags` or `usageStats`. Both are tracked as potential follow-ups.

### Troubleshooting

- **`401 Unauthorized`** — the PAT is invalid, expired, or the user it belongs to has been deactivated. Regenerate from Lightdash → "Personal access tokens".
- **Charts show "no lineage" in DataHub UI** — make sure your DataHub instance has the V3 lineage frontend fix from [datahub-project/datahub#17499](https://github.com/datahub-project/datahub/pull/17499) (DataHub >= v1.5.1) and that `SCHEMA_FIELD_CLL_ENABLED=true` is set on GMS.
- **Charts disappear after re-ingest** — the connector uses DataHub stateful ingestion with deletion detection by default. To keep the previous run's entities around, set `stateful_ingestion.remove_stale_metadata: false`.
