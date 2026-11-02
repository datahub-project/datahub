## Overview

Boomi Data Integration (formerly Rivery) is a cloud ELT service where you build rivers that load data from SaaS applications and databases into a warehouse, and logic rivers that orchestrate SQL, Python and other rivers. Learn more in the [Boomi Data Integration API reference](https://developer.boomi.com/docs/api/dataintegration/Rivers).

The DataHub integration for Boomi Data Integration reads the Data Integration REST API. It ingests environments and groups as containers, each river as a DataFlow, and the river's load or logic steps as DataJobs, with lineage to the target tables each river or SQL step writes. Stateful ingestion removes rivers that were deleted or filtered out.

## Concept Mapping

| Source Concept                     | DataHub Concept                                                 | Notes                                                                                      |
| ---------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `"boomi-data-integration"`         | [Data Platform](../../metamodel/entities/dataPlatform.md)       |                                                                                            |
| Account                            | Platform instance                                               | Set `platform_instance` to the account ID when you ingest more than one account.           |
| Environment                        | [Container](../../metamodel/entities/container.md)              | Subtype `Environment`.                                                                     |
| Group                              | [Container](../../metamodel/entities/container.md)              | Subtype `Group`, inside its environment.                                                   |
| River                              | [DataFlow](../../metamodel/entities/dataFlow.md)                | Subtype `River`. The URN uses the river cross ID, so renames keep the entity.              |
| Source-to-target river             | One `load` [DataJob](../../metamodel/entities/dataJob.md)       | Subtype `Load`. Outputs are the target tables.                                             |
| Action or connector executor river | One `run` DataJob                                               | Subtype `Load`. No dataset lineage.                                                        |
| Logic river step                   | One DataJob per step, named by the step ID                      | Subtype `Logic Step`. Sequential steps depend on the step before them.                     |
| Target table                       | [Dataset](../../metamodel/entities/dataset.md) on the warehouse | Referenced by URN only; ingest the warehouse with its own source plugin for full metadata. |
