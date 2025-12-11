<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

### Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |

### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.
