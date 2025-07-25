## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

This integration extracts data lineage information from the public SnapLogic Lineage API and ingests it into DataHub. It enables visibility into how data flows through SnapLogic pipelines by capturing metadata directly from the source API. This allows users to track data transformations and dependencies across their data ecosystem, enhancing observability, governance, and impact analysis within DataHub.

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrevant DataHub Concepts -->

| Source Concept | DataHub Concept                                                    | Notes                                                                                                                                   |
| -------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Snap-pack      | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | Snap-packs are mapped to Data Platforms, either directly (e.g., Snowflake) or dynamically based on connection details (e.g., JDBC URL). |
| Table/Dataset  | [Dataset](docs/generated/metamodel/entities/dataset.md)            | May be differernt. It depends on a snap type. For sql databases it's table. For kafka it's topic, etc                                   |
| Snap           | [Data Job](docs/generated/metamodel/entities/dataJob.md)           |                                                                                                                                         |
| Pipeline       | [Data Flow](docs/generated/metamodel/entities/dataFlow.md)         |                                                                                                                                         |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest lineage from snaplogic, you will need valid snaplogic credentials with access to the SnapLogic Lineage API.

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[snaplogic]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion.

#### `'acryl-datahub[snaplogic]'`

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

| Field                         | Required |            Default            | Description                                                     |
| ----------------------------- | :------: | :---------------------------: | --------------------------------------------------------------- |
| `username`                    |    ✅    |                               | SnapLogic account login                                         |
| `password`                    |    ✅    |                               | SnapLogic account password.                                     |
| `base_url`                    |    ✅    | https://elastic.snaplogic.com | Snaplogic url                                                   |
| `org_name`                    |    ✅    |                               | Organisation name in snaplogic platform                         |
| `namespace_mapping`           |    ❌    |                               | Namespace mapping. Used to map namespaces to platform instances |
| `case_insensitive_namespaces` |    ❌    |                               | List of case insensitive namespaces                             |

</details>

## Troubleshooting

### [Common Issue]
