## Overview

Informatica Intelligent Data Management Cloud (IDMC) is a cloud-native data integration and management platform. Learn more in the [official Informatica documentation](https://docs.informatica.com/integration-cloud.html).

The DataHub integration for Informatica covers projects and folders as containers, taskflows and mappings as data pipelines, and resolves table-level lineage across the data estate from mapping source/target connections. It also supports ownership extraction and stateful deletion detection.

## Concept Mapping

| Source Concept  | DataHub Concept                                           | Notes                       |
| --------------- | --------------------------------------------------------- | --------------------------- |
| `"informatica"` | [Data Platform](../../metamodel/entities/dataPlatform.md) |                             |
| Project         | [Container](../../metamodel/entities/container.md)        | SubType `"Project"`         |
| Folder          | [Container](../../metamodel/entities/container.md)        | SubType `"Folder"`          |
| Taskflow        | [DataFlow](../../metamodel/entities/dataFlow.md)          | SubType `"Taskflow"`        |
| Mapping         | [DataJob](../../metamodel/entities/dataJob.md)            | SubType `"Mapping"`         |
| Mapping Task    | [DataJob](../../metamodel/entities/dataJob.md)            | SubType `"Mapping Task"`    |
| Source/Target   | [Dataset](../../metamodel/entities/dataset.md)            | Upstream/downstream lineage |
