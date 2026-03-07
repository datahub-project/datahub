### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |

#### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
