### Capabilities

#### Lineage Behavior

By default, the connector emits lineage from MicroStrategy datasets to visualizations by setting `ChartInfo.inputs` and `ChartInfo.inputEdges`. When visualization metadata exposes metric and attribute references, it also emits chart `InputFields` pointing at the MicroStrategy dataset fields used by each visualization. It does not emit direct dataset-to-dashboard edges because those edges can make DataHub lineage views noisy for large BI dashboards.

Set `emit_dashboard_dataset_edges: true` if you want every dashboard dataset to appear directly upstream of the dashboard as fallback lineage.

The definition APIs do not expose a visualization's dataset binding directly, so the connector resolves it in tiers. First it reads the modeling document API: derived metrics and attributes are scoped to a single dataset, so a visualization grid referencing them identifies its source dataset with certainty. When no dataset-scoped objects are referenced (or modeling access is unavailable), the connector falls back to inferring the binding from shared object references and name tokens. If that inference cannot exclude any dataset (for example, dashboards built from one cube per time period where all cubes share one object catalog), the visualization is treated as unresolved and gets no dataset inputs rather than an all-to-all fan-out; use `emit_dashboard_dataset_edges: true` to keep such dashboards connected to their datasets.

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

#### Metric and Attribute Tags

MicroStrategy metrics and attributes are emitted as schema fields on the dashboard dataset/cube. The connector attaches canonical DataHub tags to the fields:

- `urn:li:tag:Measure` for metrics.
- `urn:li:tag:Dimension` for attributes and attribute forms.
- `urn:li:tag:Temporal` for date/time attribute forms.

These tags are written to source-managed `SchemaMetadata` field metadata, not editable schema metadata.

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
