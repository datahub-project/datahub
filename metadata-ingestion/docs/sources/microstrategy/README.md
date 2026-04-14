## Overview

[MicroStrategy](https://www.microstrategy.com/) is an enterprise business intelligence platform
for analytics, dashboards, and reporting. It lets organizations build and share reports and
interactive dossiers backed by data from warehouse and operational sources.

The DataHub connector ingests projects, folders, dashboards (dossiers), reports, intelligent
cubes, and datasets from MicroStrategy's REST API. It supports lineage from warehouse tables to
cubes, ownership extraction, and stateful deletion of removed entities.

## Concept Mapping

| Source Concept      | DataHub Concept                                           | Notes                                     |
| ------------------- | --------------------------------------------------------- | ----------------------------------------- |
| `"MicroStrategy"`   | [Data Platform](../../metamodel/entities/dataPlatform.md) |                                           |
| Project             | [Container](../../metamodel/entities/container.md)        | SubType `"Project"`                       |
| Folder              | [Container](../../metamodel/entities/container.md)        | SubType `"Folder"`                        |
| Dashboard (Dossier) | [Dashboard](../../metamodel/entities/dashboard.md)        |                                           |
| Report              | [Chart](../../metamodel/entities/chart.md)                | Reports are represented as Chart entities |
| Intelligent Cube    | [Dataset](../../metamodel/entities/dataset.md)            | SubType `"Cube"`                          |
| Dataset             | [Dataset](../../metamodel/entities/dataset.md)            |                                           |
| User (Owner)        | [CorpUser](../../metamodel/entities/corpuser.md)          | Extracted via `include_ownership: true`   |
