## Lineage Behavior

By default, the connector emits lineage from MicroStrategy datasets to visualizations by setting `ChartInfo.inputs` and `ChartInfo.inputEdges`. When visualization metadata exposes metric and attribute references, it also emits chart `InputFields` pointing at the MicroStrategy dataset fields used by each visualization. It does not emit direct dataset-to-dashboard edges because those edges can make DataHub lineage views noisy for large BI dashboards.

Set `emit_dashboard_dataset_edges: true` if you want every dashboard dataset to appear directly upstream of the dashboard as fallback lineage.

When `extract_visualization_details: true`, the connector creates a dashboard instance and calls the v2 visualization definition endpoint to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. Use `dashboard_pattern` to scope live validation runs, for example:

```yaml
dashboard_pattern:
  allow:
    - "^Sales Performance$"
```

## Reports

Set `extract_reports: true` to ingest MicroStrategy reports as DataHub chart entities with the `Report` subtype. Report extraction is disabled by default because report libraries can be much larger than curated dossiers. Use `report_pattern` to scope report extraction.

When report definitions expose source and `availableObjects` metadata, the connector emits a report-scoped MicroStrategy source dataset containing the report metrics, attributes, and attribute forms. Report lineage uses `ChartInfo.inputs`, `ChartInfo.inputEdges`, and chart `InputFields` from that report source dataset to the report chart.

If `extract_dashboard_dependencies: true` and `extract_reports: true`, dashboards that expose report dependencies link to the matching report chart entities. Reports are a separate lineage path from dossier visualizations:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset
Dashboard/Dossier -> Report -> MicroStrategy Report Source Dataset
```

Set `extract_report_sql_lineage: true` only when you also want optional coarse report source dataset -> warehouse table lineage from the report SQL-view API. This setting is disabled by default and does not emit direct warehouse edges to reports or dashboards.

## Source Warehouses

When `extract_source_warehouses: true`, the connector calls MicroStrategy datasource management APIs for each project and records a source warehouse summary on the project container. The summary includes the datasource count, source database types, datasource types, and DBMS names exposed by MicroStrategy.

If a dashboard dataset payload includes a direct source warehouse reference, the connector also records datasource ID, datasource name, source type, database version, DBMS name, connection ID/name, and available database/schema context as dataset custom properties.

When `extract_warehouse_lineage: true`, the connector executes the dashboard/dossier SQL-view API and emits coarse upstream lineage from each MicroStrategy dataset to the physical warehouse datasets parsed from SQL. It does not store raw SQL or connection strings. This setting is disabled by default because SQL-view lineage is table-level and does not prove field-level metric, attribute, or fact lineage. The connector uses dataset-level source warehouse metadata when MicroStrategy provides it. It only falls back to project-level datasource metadata when the project resolves to one unambiguous warehouse context, so multi-source projects do not get broad dataset-to-table edges from an arbitrary datasource. The resulting lineage shape is:

```text
Dashboard/Dossier -> Visualization -> MicroStrategy Dataset -> Warehouse Dataset
```

The connector intentionally keeps direct `DashboardInfo.datasetEdges` disabled by default so dashboards do not draw edges directly to every dataset in DataHub lineage views.

## Dependency and Model Lineage Enrichment

When `extract_dashboard_dependencies: true`, the connector uses MicroStrategy metadata search lineage APIs to record direct dashboard component dependency summaries, including dependency counts by MicroStrategy object type.

When `extract_metric_expressions: true`, the connector fetches accessible metric model definitions and stores expression token summaries in metric field `jsonProps`.

When `extract_model_lineage: true`, the connector probes modeling table APIs needed for logical table and physical source warehouse lineage. Missing privileges are reported as warnings and counters; the connector continues with dashboard, dataset, metric, and source warehouse metadata.

## Metric and Attribute Tags

MicroStrategy metrics and attributes are emitted as schema fields on the dashboard dataset/cube. The connector attaches canonical DataHub tags to the fields:

- `urn:li:tag:Measure` for metrics.
- `urn:li:tag:Dimension` for attributes and attribute forms.
- `urn:li:tag:Temporal` for date/time attribute forms.

These tags are written to source-managed `SchemaMetadata` field metadata, not editable schema metadata.
