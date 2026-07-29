## Overview

[Cube](https://cube.dev/) is a headless semantic layer that defines metrics, dimensions, and joins once and exposes them to BI tools, data apps, and AI agents through SQL, REST, and GraphQL APIs. Its data model is organised into **cubes** (business entities such as orders or customers) and **views** (curated, query-ready datasets built on top of cubes).

This source ingests the Cube data model into DataHub as datasets: each cube and view becomes a dataset whose measures and dimensions are modelled as schema fields. It supports both **Cube Core** (self-hosted, via the `/v1/meta` REST endpoint) and **Cube Cloud**, where it merges `/v1/meta` (structural and presentation metadata) with the richer **Metadata API** (warehouse and column-level lineage). On Cube Cloud the connector can mint the metadata-scoped token automatically via the Control Plane API. DataHub captures descriptions, measure/dimension classification, view-to-cube lineage, and — where the deployment exposes it — column-level lineage down to the underlying warehouse tables. On Cube Cloud it also ingests saved **reports** as charts and **workbooks** as dashboards via the Platform API, extending lineage to the BI consumption layer. Stateful ingestion removes cubes and views that have been deleted from the model.

## Concept Mapping

| Source Concept                                          | DataHub Concept                                                                   | Notes                                                |
| ------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Deployment / data model                                 | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container) | Subtype `Cube Deployment`                            |
| Cube                                                    | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | Subtype `Cube`                                       |
| View                                                    | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | Subtype `View`                                       |
| Measure                                                 | Schema Field                                                                      | Tagged `Measure`; aggregation in native type         |
| Dimension                                               | Schema Field                                                                      | Tagged `Dimension`; primary keys marked as key       |
| `format` / `drillMembers` / `cumulative`                | Schema Field `jsonProps`                                                          | Measure presentation hints                           |
| `joins` / `hierarchies` / `folders` / `preAggregations` | Dataset custom properties                                                         | Structural model metadata                            |
| `public` / `isVisible`                                  | Ingestion filter                                                                  | Hidden cubes/members skipped unless `include_hidden` |
| `table_references` / cube `sql`                         | [Lineage](https://docs.datahub.com/docs/features/feature-guides/lineage)          | Lineage to upstream warehouse tables                 |
| View member `aliasMember`                               | Fine-Grained Lineage                                                              | Column-level view-to-cube lineage                    |
| `meta`                                                  | Tags / Terms / Owners / Domains                                                   | Mapped via `meta_mapping` / `column_meta_mapping`    |
| Report (Cube Cloud)                                     | [Chart](https://docs.datahub.com/docs/generated/metamodel/entities/chart)         | Input lineage to queried cubes/views                 |
| Workbook (Cube Cloud)                                   | [Dashboard](https://docs.datahub.com/docs/generated/metamodel/entities/dashboard) | Contains its reports' charts                         |
