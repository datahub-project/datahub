## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

The Datahub Pulsar source plugin extracts `topic` and `schema` metadata from an Apache Pulsar instance and ingest the information into Datahub. The plugin uses the [Pulsar admin Rest API interface](https://pulsar.apache.org/admin-rest-api/#) to interact with the Pulsar instance. The following APIs are used in order to:
- [Get the list of existing tenants](https://pulsar.apache.org/admin-rest-api/#tag/tenants)
- [Get the list of namespaces associated with each tenant](https://pulsar.apache.org/admin-rest-api/#tag/namespaces)
- [Get the list of topics associated with each namespace](https://pulsar.apache.org/admin-rest-api/#tag/persistent-topic)
    - persistent topics
    - persistent partitioned topics
    - non-persistent topics
    - non-persistent partitioned topics
- [Get the latest schema associated with each topic](https://pulsar.apache.org/admin-rest-api/#tag/schemas)

The data is extracted on `tenant` and `namespace` basis, topics with corresponding schema (if available) are ingested as [Dataset](docs/generated/metamodel/entities/dataset.md) into Datahub. Some additional values like `schema description`, `schema_version`, `schema_type` and `partitioned` are included as `DatasetProperties`.


### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->


| Source Concept | DataHub Concept                                                    | Notes                                                                     |
|----------------|--------------------------------------------------------------------|---------------------------------------------------------------------------|
| `pulsar`       | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |                                                                           |
| Pulsar Topic   | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `topic`                                                        |
| Pulsar Schema  | [SchemaField](docs/generated/metamodel/entities/schemaField.md)    | Maps to the fields defined within the `Avro` or `JSON` schema definition. | 


## Metadata Ingestion Quickstart

For context on getting started with ingestion, check out our [metadata ingestion guide](../../../../metadata-ingestion/README.md).
