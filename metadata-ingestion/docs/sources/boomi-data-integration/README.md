## Overview

Boomi Data Integration (formerly Rivery) is a cloud ELT service where you build rivers that load data from SaaS applications and databases into a warehouse, and logic rivers that orchestrate SQL, Python and other rivers. Learn more in the [Boomi Data Integration API reference](https://developer.boomi.com/docs/api/dataintegration/Rivers).

The DataHub integration for Boomi Data Integration reads the Data Integration REST API. It ingests environments and groups as containers, each river as a DataFlow, the river's load, run or logic steps as DataJobs, and recent river runs as DataProcessInstances, with lineage from the database tables or SQL a river reads to the tables it writes, column lineage where the river or its SQL names both columns, and, optionally, river owners. Tables or columns that would need guessing are skipped and counted in the ingestion report. Stateful ingestion removes rivers that were deleted or filtered out.

## Concept Mapping

| Source Concept                     | DataHub Concept                                                        | Notes                                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `"boomi-data-integration"`         | [Data Platform](../../metamodel/entities/dataPlatform.md)              |                                                                                                                            |
| Account                            | Platform instance                                                      | Set `platform_instance` to the account ID when you ingest more than one account.                                           |
| Environment                        | [Container](../../metamodel/entities/container.md)                     | Subtype `Environment`.                                                                                                     |
| Group                              | [Container](../../metamodel/entities/container.md)                     | Subtype `Group`, inside its environment.                                                                                   |
| River                              | [DataFlow](../../metamodel/entities/dataFlow.md)                       | Subtype `River`. The URN uses the river cross ID, so renames keep the entity.                                              |
| Source-to-target river             | One `load` [DataJob](../../metamodel/entities/dataJob.md)              | Subtype `Load`. Inputs are the source tables, outputs the target tables.                                                   |
| Action or connector executor river | One `run` DataJob                                                      | Subtype `Run`. No dataset lineage.                                                                                         |
| Logic river step                   | One DataJob per step, named by the step ID                             | Subtype `Logic Step`. Sequential steps depend on the step before them.                                                     |
| River run                          | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Rows sharing a `run_group_id` are one run. Logic river runs belong to the DataFlow, other runs to the `load` or `run` job. |
| Source or target table             | [Dataset](../../metamodel/entities/dataset.md)                         | Referenced by URN only; ingest the database with its own source plugin for full metadata.                                  |
| River creator and last editor      | Owners of the DataFlow and its DataJobs                                | Opt-in with `extract_owners`.                                                                                              |
