### Capabilities

#### Lineage Behavior

By default, the connector emits lineage from MicroStrategy datasets to visualizations by setting `ChartInfo.inputs` and `ChartInfo.inputEdges`. When visualization metadata exposes metric and attribute references, it also emits chart `InputFields` pointing at the MicroStrategy dataset fields used by each visualization. It does not emit direct dataset-to-dashboard edges because those edges can make DataHub lineage views noisy for large BI dashboards.

Set `emit_dashboard_dataset_edges: true` if you want every dashboard dataset to appear directly upstream of the dashboard as fallback lineage.

The definition APIs do not expose a visualization's dataset binding directly, so the connector resolves it in tiers. First it reads the modeling document API: derived metrics and attributes are scoped to a single dataset, so a visualization grid referencing them identifies its source dataset with certainty. For compound-grid visualizations, the connector then binds each column group (`columnSets`) to its backing dataset using the page name, group-name tokens, the group's member object IDs, and — when the page's datasets correspond one-to-one with the groups — elimination for the leftover pair; a partially bound grid keeps its resolved groups and counts the rest in the `column_sets_unbound` report counter. When neither applies, the connector falls back to inferring the binding from shared object references and name tokens. If that inference cannot exclude any dataset (for example, dashboards built from one cube per time period where all cubes share one object catalog), the visualization is treated as unresolved and gets no dataset inputs rather than an all-to-all fan-out; use `emit_dashboard_dataset_edges: true` to keep such dashboards connected to their datasets.

When `extract_visualization_details: true`, the connector creates a dashboard instance and calls the v2 visualization definition endpoint to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. Use `dashboard_pattern` to scope live validation runs, for example:

```yaml
dashboard_pattern:
  allow:
    - "^Quarterly Business Review$"
```

#### Reports

Set `extract_reports: true` to ingest MicroStrategy reports as DataHub chart entities with the `Report` subtype. Report extraction is disabled by default because report libraries can be much larger than curated dossiers. Use `report_pattern` to scope report extraction.

When dashboards are also extracted, only reports referenced by an ingested dashboard are ingested by default, so scoping dashboards with `dashboard_pattern` scopes reports too. Linked reports are fetched directly by id rather than by enumerating the project's report library, so large report libraries do not slow down scoped runs. Set `extract_independent_reports: true` to also ingest reports not used by any dashboard (every report matching `report_pattern`); this enumerates the full report library. This scoping relies on `extract_dashboard_dependencies` for the dashboard-to-report linkage; without it, or without dashboard extraction, all matching reports are ingested.

When report definitions expose source and `availableObjects` metadata, the connector emits a report-scoped MicroStrategy source dataset containing the report metrics, attributes, and attribute forms. Report lineage uses `ChartInfo.inputs`, `ChartInfo.inputEdges`, and chart `InputFields` from that report source dataset to the report chart.

If `extract_dashboard_dependencies: true` and `extract_reports: true`, dashboards that expose report dependencies link to the matching report chart entities. Reports are a separate lineage path from dossier visualizations:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset
Dashboard/Dossier -> Report -> MicroStrategy Report Source Dataset
```

Set `extract_report_sql_lineage: true` only when you also want optional coarse report source dataset -> warehouse table lineage from the report SQL-view API. This setting is disabled by default and does not emit direct warehouse edges to reports or dashboards.

#### Source Warehouses

When `extract_source_warehouses: true`, the connector calls MicroStrategy datasource management APIs for each project and records a source warehouse summary on the project container. The summary includes the datasource count, source database types, datasource types, and DBMS names exposed by MicroStrategy.

If a dashboard dataset payload includes a direct source warehouse reference, the connector also records datasource ID, datasource name, source type, database version, DBMS name, connection ID/name, and available database/schema context as dataset custom properties.

When `extract_warehouse_lineage: true`, the connector executes the dashboard/dossier SQL-view API and emits coarse upstream lineage from each MicroStrategy dataset to the physical warehouse datasets parsed from SQL. When field-level model lineage resolves for a dataset, its table-level upstreams are restricted to the tables evidenced by field lineage — tables the SQL only joins for filtering (dimension lookups, calendar subqueries) are not emitted as upstreams. Datasets without field-level lineage keep the full SQL-derived table set. It does not store raw SQL or connection strings. This setting is disabled by default because SQL-view lineage is table-level and does not prove field-level metric, attribute, or fact lineage. The connector uses dataset-level source warehouse metadata when MicroStrategy provides it. It only falls back to project-level datasource metadata when the project resolves to one unambiguous warehouse context, so multi-source projects do not get broad dataset-to-table edges from an arbitrary datasource. The resulting lineage shape is:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset -> Warehouse Dataset
```

The connector intentionally keeps direct `DashboardInfo.datasetEdges` disabled by default so dashboards do not draw edges directly to every dataset in DataHub lineage views.

#### Dependency and Model Lineage Enrichment

When `extract_dashboard_dependencies: true`, the connector uses MicroStrategy metadata search lineage APIs to record direct dashboard component dependency summaries, including dependency counts by MicroStrategy object type.

When `extract_metric_expressions: true`, the connector fetches accessible metric model definitions and stores expression token summaries in metric field `jsonProps`.

When `extract_model_lineage: true`, the connector probes modeling table APIs needed for logical table and physical source warehouse lineage. Missing privileges are reported as warnings and counters; the connector continues with dashboard, dataset, metric, and source warehouse metadata.

#### Folder Navigation

The connector builds its browse hierarchy by walking each object's MicroStrategy folder ancestry, which reflects the metadata layer's internal folder names rather than the curated grouping Strategy Web's Library shows (favorites, "My Reports" vs. "Shared Reports", hidden system folders). With `use_predefined_folder_names: true` (the default), the connector resolves well-known folders via `GET /api/folders/preDefined` and reshapes the hierarchy to match Strategy Web: the folder Strategy Web calls "Shared Reports" (internally named "Reports") gets its MicroStrategy-assigned label everywhere — container identity (URN), `folder_pattern` matching, and display — and two system containers Strategy Web never shows are omitted entirely, with their children re-parented to the nearest kept ancestor: the project root folder (named after the project, which would render as a duplicate project level) and "Public Objects". Set `use_predefined_folder_names: false` to keep the raw metadata hierarchy (for example if you already ingested these folders and want to avoid the resulting container URN changes; see `docs/how/updating-datahub.md`).

To exclude personal folders from ingestion (MicroStrategy's per-user "My Reports", nested under each user's own profile folder), use `folder_pattern`:

```yaml
folder_pattern:
  deny:
    - "^My Reports$"
```

Every user's personal folder carries that same literal name, so this excludes them all without needing per-user configuration.

#### Metric and Attribute Tags

MicroStrategy metrics and attributes are emitted as schema fields on the dashboard dataset/cube. The connector attaches canonical DataHub tags to the fields:

- `urn:li:tag:Measure` for metrics.
- `urn:li:tag:Dimension` for attributes and attribute forms.
- `urn:li:tag:Temporal` for date/time attribute forms.
- `urn:li:tag:Derived` for visualization-local derived metrics (see below).

These tags are written to source-managed `SchemaMetadata` field metadata, not editable schema metadata.

#### Derived Metrics and Column Groups

Compound-grid dossier visualizations can define **derived metrics** directly in the grid (grid columns marked `derived: true`, such as an inline percent-to-plan calculation). These exist only inside the visualization template — they are not metadata catalog objects, and MicroStrategy's REST API exposes no formula or model definition for them. With `extract_derived_metrics: true` (the default), the connector surfaces each one as a schema field on the dataset backing its column group, tagged `Measure` + `Derived`, with a description naming the column group and source visualization. Derived metrics whose column group cannot be attributed to a dataset are counted in the `derived_metrics_unattached` report counter rather than silently dropped. This feature reads the runtime grids already fetched for lineage, so it requires `extract_lineage` and `extract_visualization_details` and adds no API calls.

Compound grids also organize their columns into named **column groups** (`columnSets`, e.g. one group per business domain), often repeating the same metric names per group with different logic behind each — the grouping is what tells a reader what they are looking at. The connector surfaces this two ways: the chart's `microstrategyColumnGroups` custom property records each group's member metrics and backing dataset, and grouped schema fields carry a `microstrategyColumnGroup` entry in their field `jsonProps`.

#### Metric Formula Lineage

Set `extract_metric_formula_lineage: true` to parse `{Metric Name}` references out of catalog metric expressions (fetched via `extract_metric_expressions`) and emit field-to-field lineage from each metric to the sibling fields it references on the same dataset. This is best effort: references to objects that are not fields of the dataset (for example, a catalog metric not exposed on the cube) are counted in the `metric_formula_refs_unresolved` report counter and skipped. Disabled by default.

#### Usage Statistics

Set `extract_usage_statistics: true` to emit daily view counts, unique-user counts, and per-user usage for ingested dashboards and reports. MicroStrategy has no per-object usage REST endpoint; the connector queries the Platform Analytics telemetry cube (the `Platform Analytics (Agg)` cube in the `Platform Analytics` project) through the standard cube instance APIs and joins the telemetry rows to ingested entities by object GUID.

Requirements and behavior:

- Platform Analytics must be enabled on the environment (standard on MicroStrategy Cloud) and the ingestion principal needs read access to the Platform Analytics project. When the project or cube is missing, the connector records one warning and continues without usage.
- The cube's attributes and metrics are resolved by name (`Date`, `Project`, `Object`, `User`, and `Num Executions` or `Count Actions`), so renamed or heavily customized telemetry cubes are skipped with a warning explaining what was missing. Use `usage_cube_name` to point at a custom cube that exposes the same objects.
- `usage_lookback_days` bounds the request window (default 14 days — the shipped aggregate cube typically retains a 14-day rolling window). Usage freshness depends on the environment's Platform Analytics cube refresh schedule.
- Usage rows for objects outside the ingested scope are counted in the `usage_objects_unmatched` report counter and skipped.

### Limitations

- Warehouse lineage from the SQL-view APIs is coarse table-level lineage and is disabled by default (`extract_warehouse_lineage` and `extract_report_sql_lineage`). Field-level metric, attribute, or fact lineage to warehouse tables is not available yet.
- Report extraction is disabled by default (`extract_reports`) because report libraries can be much larger than curated dossiers.
- Direct dashboard-to-dataset edges are disabled by default; enable `emit_dashboard_dataset_edges` only if you want dashboard-level fallback lineage, which can make lineage views noisy for large dashboards.
- Modeling APIs (logical tables, metric expressions) may return 403 when the principal lacks modeling privileges. The connector degrades gracefully — missing privileges are reported as warnings and counters, and ingestion continues with dashboard, dataset, metric, and source warehouse metadata.
- Multi-source projects only receive dataset-to-warehouse edges when MicroStrategy exposes dataset-level source warehouse metadata; the connector does not guess a datasource when the project-level warehouse context is ambiguous.
- Dataset `upstreamLineage` is replaced wholesale while any upstream tables remain, but a dataset whose warehouse lineage disappears entirely keeps the previous run's aspect (stale-entity removal deletes entities, not aspects). Remove leftovers with `datahub delete --urn <urn> --aspect upstreamLineage` or a rollback of the earlier run. Avoid pipeline-level incremental-lineage transformers with this source: their patch-add semantics prevent edge reductions from propagating.

### Troubleshooting

#### Missing dataset-to-visualization lineage

If charts do not show upstream datasets, the static dashboard definition may not include dataset IDs. Set `extract_visualization_details: true` so the connector creates a dashboard instance and resolves bindings from the v2 visualization definition endpoint. This requires the principal to have instance-creation privileges; use `dashboard_pattern` to scope the run while validating.

#### 403 errors on modeling or SQL-view APIs

The connector does not fail ingestion on modeling API 403s — it records warnings and counters in the ingestion report and continues. Check the report counters to see which APIs were inaccessible, and grant the principal instance-creation and SQL-view access (for warehouse lineage) or modeling privileges (for `extract_metric_expressions` / `extract_model_lineage`) as needed.

#### Session invalidation mid-run

MicroStrategy can invalidate the session token at any time (idle or absolute timeouts, concurrent-session limits, administrator action). The connector re-authenticates automatically and replays the failed request; the `sessions_reauthenticated` counter in the ingestion report shows when this happened. If re-login itself fails — or the server rejects a request immediately after a successful re-login — the run aborts with a single `MicroStrategy Authentication Lost` failure instead of failing every remaining project. Avoid signing in elsewhere with the ingestion service account while a run is in progress if your tenant enforces concurrent-session limits.

#### Empty or incomplete results

If little or no metadata is ingested, verify that the principal has Library API access and project-scoped metadata search, and check that `project_pattern` (and `dashboard_pattern` / `report_pattern`, if set) are not filtering out the content you expect. Guest authentication works for public demo exploration but does not expose all modeling APIs.
