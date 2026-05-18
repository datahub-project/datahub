### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Metric Views

[Unity Catalog Metric Views](https://docs.databricks.com/aws/en/metric-views/) are first-class semantic layer assets that expose dimensions and measures via a YAML specification. DataHub ingests them as datasets with subtype `Metric View` when you opt in with `include_metric_views: true`.

```yaml
source:
  type: unity-catalog
  config:
    include_metric_views: true
    metric_view_pattern:
      allow:
        - my_catalog\.analytics\..*
```

When enabled, each metric view emits:

- A `Metric View` subtype so it is distinguishable from regular tables and views in the UI.
- A `ViewProperties` aspect carrying the raw YAML body with `viewLanguage: YAML`. The `materialized` flag is set to `true` when the YAML contains `materialization: materialized`.
- Upstream lineage parsed from the YAML `source` and `joins[].source` fields. Both 3-part (`catalog.schema.table`) and 2-part (`schema.table`, resolved against the metric view's own catalog) identifiers are supported, as are backtick-quoted parts (`` `db.with.dots`.schema.table ``).
- Column-level lineage parsed from each `dimensions[].expr` / `measures[].expr` using the Databricks SQL dialect (requires `include_column_lineage: true`, which is the default). Unqualified columns map to the source table; `<join_name>.column` references map through the join's `source`.
- A `Dimension` or `Measure` tag on each schema field whose name matches a YAML `dimensions[].name` or `measures[].name`.
- A `metric_view.filter` custom property when the YAML carries a top-level `filter`.
- Per-column descriptions taken from the YAML `dimensions[].description` / `measures[].description` when present (falls back to the underlying Unity Catalog column comment otherwise).
- A filtered set of custom properties: the Spark engine config snapshot Unity Catalog injects as `view.sqlConfig.spark.*` keys (~150 entries per view) is dropped, since it is identical across views in a workspace and crowds the UI. All other source-table properties are preserved unchanged.

If a metric view's `source` is a SQL subquery, or if it uses a 1-part identifier that DataHub can't resolve, the YAML lineage path is skipped and DataHub falls back to the Unity Catalog table-lineage REST API for upstream resolution.

`include_metric_views` is `false` by default for backwards compatibility — when the flag is off (or when the installed `databricks-sdk` predates `TableType.METRIC_VIEW`), metric views continue to be emitted as plain `Table` entities with no view body.

#### Advanced

##### Multiple Databricks Workspaces

If you have multiple databricks workspaces **that point to the same Unity Catalog metastore**, our suggestion is to use separate recipes for ingesting the workspace-specific Hive Metastore catalog and Unity Catalog metastore's information schema.

To ingest Hive metastore information schema

- Setup one ingestion recipe per workspace
- Use platform instance equivalent to workspace name
- Ingest only hive_metastore catalog in the recipe using config `catalogs: ["hive_metastore"]`

To ingest Unity Catalog information schema

- Disable hive metastore catalog ingestion in the recipe using config `include_hive_metastore: False`
- Ideally, just ingest from one workspace
- To ingest from both workspaces (e.g. if each workspace has different permissions and therefore restricted view of the UC metastore):
  - Use same platform instance for all workspaces using same UC metastore
  - Ingest usage from only one workspace (you lose usage from other workspace)
  - Use filters to only ingest each catalog once, but shouldn’t be necessary

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

#### Lineage extraction is too slow

Unity Catalog REST API requires one call per table (table lineage) and one call per column (column lineage). To improve performance, disable column lineage with `include_column_lineage: false`.
