## Prerequisites

Create a MicroStrategy user with read access to the projects and dossiers you want to ingest. For production metadata extraction, use username/password authentication with access to Library APIs and project-scoped metadata search. Guest authentication is useful for public demo exploration but does not expose all modeling APIs.

For optional upstream warehouse table lineage, the MicroStrategy principal needs access to create a dashboard/dossier instance and read the dataset SQL-view API. This lineage is coarse table-level lineage and is disabled by default until field-level metric, attribute, or fact lineage is available. Modeling privileges such as Architect/editor access can expose additional logical model details, but they are not required for SQL-view physical table lineage when the SQL-view APIs are available.

## Capabilities

- Projects and folders as containers.
- Dossiers/documents as dashboards.
- Visualizations as charts.
- Embedded dashboard datasets/cubes as datasets.
- Project source warehouses/datasources as project metadata properties.
- Metric fields tagged as `Measure`.
- Attribute fields tagged as `Dimension`.
- Date/time attribute forms tagged as `Temporal`.
- Dataset-to-visualization lineage via chart inputs.
- Optional coarse dataset-to-warehouse lineage via dashboard/dossier SQL-view APIs.
- Optional dashboard-level dataset edges through `emit_dashboard_dataset_edges`.
- Stateful ingestion stale-entity removal.
