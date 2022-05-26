### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| `iceberg` | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | |
| Table | [Dataset](docs/generated/metamodel/entities/dataset.md) | Each Iceberg table maps to a Dataset named using the parent folders.  If a table is stored under `my/namespace/table`, the dataset name will be `my.namespace.table`.  If a [Platform Instance](https://datahubproject.io/docs/platform-instances/) is configured, it will be used as a prefix: `<platform_instance>.my.namespace.table`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties) | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | The value of a table property can be used as the name of a CorpUser owner.  This table property name can be configured with the source option `user_ownership_property`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties) | CorpGroup | The value of a table property can be used as the name of a CorpGroup owner.  This table property name can be configured with the source option `group_ownership_property`. |
| Table parent folders (excluding [warehouse catalog location](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)) | Container | Available in a future release | 
| [Table schema](https://iceberg.apache.org/spec/#schemas-and-data-types) | SchemaField | Maps to the fields defined within the Iceberg table schema definition. | 

## Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]
