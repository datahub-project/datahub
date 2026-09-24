## Overview

Informatica Intelligent Data Management Cloud (IDMC) is a cloud-native data integration and management platform. Learn more in the [official Informatica documentation](https://docs.informatica.com/integration-cloud.html).

The DataHub integration for Informatica covers projects and folders as containers; Mapping Tasks as DataFlows with a `transform` DataJob per task; Taskflows as DataFlows with a single `orchestrate` DataJob that chains the step order via `inputDatajobs`; and resolves table-level lineage across the data estate from mapping source/target connections. It also supports ownership extraction and stateful deletion detection.

## Concept Mapping

| Source Concept  | DataHub Concept                                                                                                     | Notes                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `"informatica"` | [Data Platform](../../metamodel/entities/dataPlatform.md)                                                           |                                                                                                                                   |
| Project         | [Container](../../metamodel/entities/container.md)                                                                  | SubType `"Project"`                                                                                                               |
| Folder          | [Container](../../metamodel/entities/container.md)                                                                  | SubType `"Folder"`                                                                                                                |
| Taskflow        | [DataFlow](../../metamodel/entities/dataFlow.md) + one `orchestrate` [DataJob](../../metamodel/entities/dataJob.md) | SubTypes `"Taskflow"` / `"Taskflow Orchestration"`; the orchestrate sits at the end of the chain with `inputDatajobs = [last MT]` |
| Mapping Task    | [DataFlow](../../metamodel/entities/dataFlow.md) + inner `transform` [DataJob](../../metamodel/entities/dataJob.md) | SubTypes `"Mapping Task"` / `"Task Logic"`; MTs chain to each other via `inputDatajobs` in Taskflow step order                    |
| Mapping         | _not emitted as a standalone entity_                                                                                | Only Mapping Tasks (runnable schedules) are emitted; the Mapping reference is surfaced via customProperties on the Task           |
| Mapplet         | _not emitted_                                                                                                       | Internal sub-mappings included in other mappings; skipped                                                                         |
| Source/Target   | [Dataset](../../metamodel/entities/dataset.md)                                                                      | Upstream/downstream lineage; external dataset URNs receive a minimal `Status` stub so they resolve in lineage search              |
