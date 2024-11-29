### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| `iceberg` | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | |
| Table | [Dataset](docs/generated/metamodel/entities/dataset.md) | An Iceberg table is registered inside a catalog using a name, where the catalog is responsible for creating, dropping and renaming tables.  Catalogs manage a collection of tables that are usually grouped into namespaces.  The name of a table is mapped to a Dataset name.  If a [Platform Instance](https://datahubproject.io/docs/platform-instances/) is configured, it will be used as a prefix: `<platform_instance>.my.namespace.table`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties) | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | The value of a table property can be used as the name of a CorpUser owner.  This table property name can be configured with the source option `user_ownership_property`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties) | CorpGroup | The value of a table property can be used as the name of a CorpGroup owner.  This table property name can be configured with the source option `group_ownership_property`. |
| Table parent folders (excluding [warehouse catalog location](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)) | Container | Available in a future release | 
| [Table schema](https://iceberg.apache.org/spec/#schemas-and-data-types) | SchemaField | Maps to the fields defined within the Iceberg table schema definition. | 

## Troubleshooting

### Exceptions while increasing `processing_threads`

Each processing thread will open several files/sockets to download manifest files from blob storage. If you experience
exceptions appearing when increasing `processing_threads` configuration parameter, try to increase limit of open
files (i.e. using `ulimit` in Linux).
