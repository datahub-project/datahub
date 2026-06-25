## Lineage Behavior

By default, the connector emits lineage from MicroStrategy datasets to visualizations by setting `ChartInfo.inputs`. It does not emit direct dataset-to-dashboard edges because those edges can make DataHub lineage views noisy for large BI dashboards.

Set `emit_dashboard_dataset_edges: true` if you want every dashboard dataset to appear directly upstream of the dashboard as fallback lineage.

When `extract_visualization_details: true`, the connector creates a dashboard instance and calls the v2 visualization definition endpoint to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs. Use `dashboard_pattern` to scope live validation runs, for example:

```yaml
dashboard_pattern:
  allow:
    - "^Salon Sales To Plan$"
```

## Source Warehouses

When `extract_source_warehouses: true`, the connector calls MicroStrategy datasource management APIs for each project and records a source warehouse summary on the project container. The summary includes the datasource count, source database types, datasource types, and DBMS names exposed by MicroStrategy.

If a dashboard dataset payload includes a direct source warehouse reference, the connector also records datasource ID, datasource name, source type, database version, DBMS name, connection ID/name, and available database/schema context as dataset custom properties.

When `extract_warehouse_lineage: true`, the connector executes the dashboard/dossier SQL-view API and emits coarse upstream lineage from each MicroStrategy dataset to the physical warehouse datasets parsed from SQL. It does not store raw SQL or connection strings. This setting is disabled by default because SQL-view lineage is table-level and does not prove field-level metric, attribute, or fact lineage. The resulting lineage shape is:

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
