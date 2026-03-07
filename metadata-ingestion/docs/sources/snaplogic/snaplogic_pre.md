### Overview

The `snaplogic` module ingests metadata from SnapLogic into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Integration Details

Extracts data lineage from the SnapLogic Lineage API to track data transformations and dependencies across SnapLogic pipelines.

##### Concept Mapping

This ingestion source maps the following source system concepts to DataHub concepts:

| Source Concept | DataHub Concept                                                    | Notes                                                                                                                                   |
| -------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Snap-pack      | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | Snap-packs are mapped to Data Platforms, either directly (e.g., Snowflake) or dynamically based on connection details (e.g., JDBC URL). |
| Table/Dataset  | [Dataset](docs/generated/metamodel/entities/dataset.md)            | May be different. It depends on snap type. For SQL databases it is a table, and for Kafka it is a topic.                                |
| Snap           | [Data Job](docs/generated/metamodel/entities/dataJob.md)           |                                                                                                                                         |
| Pipeline       | [Data Flow](docs/generated/metamodel/entities/dataFlow.md)         |                                                                                                                                         |

#### Metadata Ingestion Quickstart

##### Prerequisites

Requires valid SnapLogic credentials with access to the SnapLogic Lineage API.

##### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[snaplogic]'`

##### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion.

##### `'acryl-datahub[snaplogic]'`

```yml
pipeline_name: <action-pipeline-name>
source:
  type: snaplogic
  config:
    username: <snaplogic-username>
    password: <snaplogic-password>
    base_url: https://elastic.snaplogic.com
    org_name: <snaplogic-org-name>
    stateful_ingestion:
      enabled: True
      remove_stale_metadata: False
```

<details>
  <summary>View All Recipe Configuartion Options</summary>

| Field                           | Required |            Default            | Description                                                                                                                |
| ------------------------------- | :------: | :---------------------------: | -------------------------------------------------------------------------------------------------------------------------- |
| `username`                      |    ✅    |                               | SnapLogic account login                                                                                                    |
| `password`                      |    ✅    |                               | SnapLogic account password.                                                                                                |
| `base_url`                      |    ✅    | https://elastic.snaplogic.com | SnapLogic url                                                                                                              |
| `org_name`                      |    ✅    |                               | Organisation name in snaplogic platform                                                                                    |
| `create_non_snaplogic_datasets` |    ✅    |             False             | If set to `True`, the DataHub connector will automatically create non-SnapLogic datasets in DataHub when they are missing. |
| `namespace_mapping`             |    ❌    |                               | Namespace mapping. Used to map namespaces to platform instances                                                            |
| `case_insensitive_namespaces`   |    ❌    |                               | List of case insensitive namespaces                                                                                        |

</details>

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
