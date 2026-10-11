## Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

The DataHub integration for MicroStrategy ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It can also ingest MicroStrategy reports as chart entities with report-scoped source datasets when `extract_reports` is enabled. The connector records project source warehouse/source type summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations and report source datasets to reports through `ChartInfo.inputs` and `ChartInfo.inputEdges` when the connector can resolve bindings from definitions or runtime details. When metadata exposes metric and attribute references, the connector also emits chart `InputFields` for the fields used by each visualization or report. Direct dashboard dataset edges are disabled by default because large BI dashboards can reference many datasets and make lineage graphs noisy; set `emit_dashboard_dataset_edges: true` only when dashboard-level fallback lineage is preferred. Coarse table-level warehouse lineage from SQL-view APIs is also disabled by default; set `extract_warehouse_lineage: true` for dashboard dataset -> warehouse table edges or `extract_report_sql_lineage: true` for report source dataset -> warehouse table edges before field-level metric, attribute, or fact lineage is available.

## Concept Mapping

| MicroStrategy Concept          | DataHub Concept                                 | Notes                                                                                                                  |
| ------------------------------ | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Project                        | Container (`Project` subtype)                   | Project-scoped APIs require `X-MSTR-ProjectID`.                                                                        |
| Folder                         | Container (`Folder` subtype)                    | Created when search results expose folder or location context.                                                         |
| Dossier / document / dashboard | Dashboard (`Dossier` subtype)                   | Dashboard metadata is read from dossier/document definition APIs.                                                      |
| Visualization                  | Chart (`Visualization` subtype)                 | Chart lineage uses `ChartInfo.inputs`, `inputEdges`, and chart `InputFields` when dataset and field IDs can be mapped. |
| Report                         | Chart (`Report` subtype)                        | Optional via `extract_reports`; report lineage uses a report-scoped source dataset.                                    |
| Dashboard dataset / cube       | Dataset (`MicroStrategy Dataset` subtype)       | Schema fields come from embedded `availableObjects`.                                                                   |
| Report source dataset          | Dataset (`MicroStrategy Dataset` subtype)       | Schema fields come from report definition `availableObjects`.                                                          |
| Source warehouse / datasource  | Custom properties and optional upstream lineage | SQL-view APIs provide opt-in coarse physical table lineage when available.                                             |
| Metric                         | Schema field with `Measure` tag                 | Metric IDs are preserved in field `jsonProps`.                                                                         |
| Attribute / form               | Schema field with `Dimension` tag               | Date/time forms also receive the `Temporal` tag.                                                                       |
| Owner                          | CorpUser ownership                              | Emitted when owner fields are exposed and `ingest_owner` is enabled.                                                   |
| Metric / attribute glossary    | Field glossary terms from explicit config maps  | The connector does not create glossary terms automatically.                                                            |
