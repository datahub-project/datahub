## Overview

Boomi Integration (the Boomi Enterprise Platform, formerly AtomSphere) is an integration platform as a service where you build processes that move data between applications, databases and files. Learn more in the [Boomi Platform API documentation](https://developer.boomi.com/docs/APIs/PlatformAPI/Introduction/Getting_started_platform).

The DataHub integration for Boomi reads the Platform API. It ingests folders as containers and each process as a DataFlow with one DataJob, along with the process description, active deployments per environment, owners taken from the process creator and last modifier, and a link back to the Boomi build page for accounts on the US and UK API hosts. Stateful ingestion removes processes that were deleted. It also removes processes that are no longer deployed, as long as `include_undeployed_processes` is `false` and the token can read deployments.

## Concept Mapping

| Source Concept | DataHub Concept                                                                                                 | Notes                                                                                  |
| -------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `"boomi"`      | [Data Platform](../../metamodel/entities/dataPlatform.md)                                                       |                                                                                        |
| Account        | Platform instance                                                                                               | Set `platform_instance` to the account ID when you ingest more than one account.       |
| Folder         | [Container](../../metamodel/entities/container.md)                                                              | Subtype `Folder`. Nested by parent folder.                                             |
| Process        | [DataFlow](../../metamodel/entities/dataFlow.md) + one `process` [DataJob](../../metamodel/entities/dataJob.md) | Subtype `Process`. The DataFlow URN uses the component ID, so renames keep the entity. |
| Deployment     | Custom property `deployments` on the DataFlow                                                                   | `environment=packageVersion` for each active deployment.                               |
| Creator        | Owner of type `DATAOWNER`                                                                                       | Boomi user names are emails, so owners become `urn:li:corpuser:<email>`.               |
| Last modifier  | Owner of type `TECHNICAL_OWNER`                                                                                 | Only added when it differs from the creator.                                           |
