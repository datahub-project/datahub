## Overview

Lightdash is an open-source business intelligence platform that turns a dbt project into an explorable semantic layer. Learn more in the [official Lightdash documentation](https://docs.lightdash.com/).

The DataHub integration for Lightdash captures BI entities such as projects, spaces, dashboards, and charts, along with chart-to-warehouse table lineage and column-level lineage from Lightdash dimensions/metrics back to the underlying warehouse columns (ClickHouse, Snowflake, BigQuery, Postgres, etc.). Ownership and stateful deletion detection are also supported.

## Concept Mapping

| Lightdash Concept   | DataHub Concept                                               | Notes                                                                                              |
| ------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `Organization`     | [Container](../../metamodel/entities/container.md)            | SubType `"Organization"` — opt-in via `include_organization_container`                              |
| `Project`          | [Container](../../metamodel/entities/container.md)            | SubType `"Project"` — one per Lightdash workspace                                                   |
| `Space`            | [Container](../../metamodel/entities/container.md)            | SubType `"Space"` — Lightdash folder, holds dashboards and charts                                   |
| `Dashboard`        | [Dashboard](../../metamodel/entities/dashboard.md)            | References its charts via `dashboardInfo.chartEdges`                                                |
| `Saved chart`      | [Chart](../../metamodel/entities/chart.md)                    | Upstream warehouse dataset surfaces in `chartInfo.inputs` + `chartInfo.inputEdges`                  |
| Lightdash dimension / metric | Chart `inputFields[].schemaFieldUrn`                | Column-level lineage from BI fields back to warehouse columns; resolved via the chart's Explore   |
| Warehouse table     | [Dataset](../../metamodel/entities/dataset.md)                | Native warehouse entity (e.g. ClickHouse, Snowflake); linked as upstream of Lightdash charts        |
| `updatedByUser`    | [CorpUser](../../metamodel/entities/corpuser.md)              | Owner emitted when `extract_owners` is enabled                                                      |
