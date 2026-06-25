## Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

The DataHub integration for MicroStrategy ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It also records project source warehouse/source type summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations through `ChartInfo.inputs` when the connector can resolve dataset bindings from static definitions or runtime visualization details. When SQL-view APIs are available, the connector also emits upstream lineage from MicroStrategy datasets to physical warehouse datasets. Direct dashboard dataset edges are disabled by default because large BI dashboards can reference many datasets and make lineage graphs noisy; set `emit_dashboard_dataset_edges: true` only when dashboard-level fallback lineage is preferred.

## Concept Mapping

| MicroStrategy Concept          | DataHub Concept                                | Notes                                                                 |
| ------------------------------ | ---------------------------------------------- | --------------------------------------------------------------------- |
| Project                        | Container (`Project` subtype)                  | Project-scoped APIs require `X-MSTR-ProjectID`.                       |
| Folder                         | Container (`Folder` subtype)                   | Created when search results expose folder or location context.        |
| Dossier / document / dashboard | Dashboard (`Dossier` subtype)                  | Dashboard metadata is read from dossier/document definition APIs.     |
| Visualization                  | Chart (`Visualization` subtype)                | Chart lineage uses `ChartInfo.inputs` when dataset IDs can be mapped. |
| Dashboard dataset / cube       | Dataset (`MicroStrategy Dataset` subtype)      | Schema fields come from embedded `availableObjects`.                  |
| Source warehouse / datasource  | Dataset upstream lineage and custom properties | SQL-view APIs provide physical table lineage when available.           |
| Metric                         | Schema field with `Measure` tag                | Metric IDs are preserved in field `jsonProps`.                        |
| Attribute / form               | Schema field with `Dimension` tag              | Date/time forms also receive the `Temporal` tag.                      |
| Owner                          | CorpUser ownership                             | Emitted when owner fields are exposed and `ingest_owner` is enabled.  |
| Metric / attribute glossary    | Field glossary terms from explicit config maps | The connector does not create glossary terms automatically.           |
