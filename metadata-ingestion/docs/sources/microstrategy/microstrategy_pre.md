### Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform for building governed dossiers, dashboards, reports, cubes, metrics, and attributes over business data.

This source ingests projects and folders as containers, dossiers/documents as dashboards, visualizations as charts, and embedded dashboard datasets/cubes as datasets with schema fields derived from MicroStrategy metrics, attributes, and attribute forms. It can also ingest MicroStrategy reports as chart entities with report-scoped source datasets when `extract_reports` is enabled, and it records project source warehouse summaries when datasource APIs are available.

Lineage is emitted from datasets to visualizations (and from report source datasets to reports) through `ChartInfo.inputs` and `ChartInfo.inputEdges`, with chart `InputFields` when metric and attribute references are exposed. Optional coarse table-level warehouse lineage from SQL-view APIs is disabled by default; enable `extract_warehouse_lineage` or `extract_report_sql_lineage` to emit dataset-to-warehouse-table edges.

### Prerequisites

Create a MicroStrategy user with read access to the projects, dossiers, documents, and reports you want to ingest. For production metadata extraction, use username/password authentication with access to Library APIs and project-scoped metadata search. Guest authentication is useful for public demo exploration but does not expose all modeling APIs.

For optional upstream warehouse table lineage, the MicroStrategy principal needs access to create a dashboard/dossier instance and read the dataset SQL-view API. This lineage is coarse table-level lineage and is disabled by default until field-level metric, attribute, or fact lineage is available. Modeling privileges such as Architect/editor access can expose additional logical model details, but they are not required for SQL-view physical table lineage when the SQL-view APIs are available.

For optional report ingestion, the principal needs project-scoped report search and report definition access. For optional report SQL lineage, it also needs permission to create report instances and read the report SQL-view API.
