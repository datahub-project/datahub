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
- A `ViewProperties` aspect carrying the raw YAML body with `viewLanguage: YAML`. The `materialized` flag is set when the YAML contains `materialization: materialized` (v0.1 string form) or any `materialization:` object (v1.1 form).
- The dataset description, taken from the YAML top-level `comment` when present (falls back to the underlying Unity Catalog table comment otherwise).
- Upstream lineage parsed from the YAML `source` and `joins[].source` fields. Both 3-part (`catalog.schema.table`) and 2-part (`schema.table`, resolved against the metric view's own catalog) identifiers are supported, as are backtick-quoted parts (`` `db.with.dots`.schema.table ``). Nested join hierarchies (snowflake-style joins) are walked recursively — every join target along the chain becomes an upstream, and every alias along the chain becomes resolvable in dimension and measure expressions.
- Column-level lineage parsed from each `dimensions[].expr` / `measures[].expr` using the Databricks SQL dialect (requires `include_column_lineage: true`, which is the default). Unqualified columns map to the source table; `<join_name>.column` references map through the join's `source`.
- Intra-view field-to-field lineage for `MEASURE(name)` composable measure references. A measure expressed as `MEASURE(total_revenue) / MEASURE(order_count)` emits two upstream edges to the `total_revenue` and `order_count` measures within the same metric view dataset. Matching is case-insensitive; the emitted URN uses the canonical case from the spec.
- A `Dimension` tag on schema fields matching a YAML `dimensions[].name`, and a `Measure` tag on those matching `measures[].name`. Measures with a non-empty `window:` block get an additional `Window Measure` tag alongside `Measure`.
- Per-column descriptions taken from the YAML `dimensions[].comment` / `measures[].comment` when present, with `description` accepted as a v0.1 fallback (falls back to the underlying Unity Catalog column comment otherwise).
- A filtered set of custom properties: the Spark engine config snapshot Unity Catalog injects as `view.sqlConfig.spark.*` keys (~150 entries per view) is dropped, since it is identical across views in a workspace and crowds the UI. All other source-table properties are preserved unchanged.

#### Metric view custom properties

The following spec-level properties are surfaced on the metric view dataset so the spec is inspectable without re-reading the YAML body:

- `metric_view.spec_version` — the spec version (e.g. `1.1`).
- `metric_view.filter` — the top-level `filter:` expression.
- `metric_view.joins` — the entire `joins:` hierarchy as a JSON string, preserving `on:` predicates, `using:` shorthand, and any nested joins.
- `metric_view.materialization.schedule` / `metric_view.materialization.mode` / `metric_view.materialization.materialized_views` — when `materialization:` is a v1.1 object, each subkey lands as a queryable property.

Per-field agent metadata (Databricks Runtime 17.3+, YAML 1.1) is exposed as dataset-level custom properties keyed `metric_view.field.<name>.*`:

- `display_name` — human-readable label for the field. Truncated to 255 characters; truncation events are counted in the ingestion report.
- `synonyms` — comma-joined alternative names. Each entry is limited to 255 characters and the list is capped at 10 entries per field (per the Databricks v1.1 spec). Per-item truncations, invalid-type drops, and the 10-cap are each recorded in the ingestion report.
- `format.type` and its subkeys (for dimensions and measures) — `number`, `currency`, `percentage`, `byte`, `date`, and `date_time` formats are supported. Each known subkey is exposed individually (including nested `decimal_places.type` and `decimal_places.places`); unknown subkeys are dropped and counted in the ingestion report.
- `window.order` / `window.range` / `window.semiadditive` for single-entry window measures, or `window` as a JSON string for multi-entry windows. Window properties are emitted on measures only.

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

Similarly, `include_table_constraints: true` adds one `tables.get()` call per non-Hive table to fetch primary key and foreign key constraints. For workspaces with thousands of tables this adds latency; leave the flag disabled (the default) if Primary Key / Foreign Key metadata is not needed.
