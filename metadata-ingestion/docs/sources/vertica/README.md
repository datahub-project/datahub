## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

This plugin extracts metadata for Tables and Views on Vertica.

This plugin is in beta and has only been tested on sample data on the Vertica database.

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrevant DataHub Concepts -->

| Source Concept | DataHub Concept                                                    | Notes |
| -------------- | ------------------------------------------------------------------ | ----- |
| `Vertica`      | [Data Platform](../../metamodel/entities/dataPlatform.md) |       |
| Table          | [Dataset](../../metamodel/entities/dataset.md)            |       |
| View           | [Dataset](../../metamodel/entities/dataset.md)            |       |

## Metadata Ingestion Quickstart

For context on getting started with ingestion, check out our [metadata ingestion guide](../../../../metadata-ingestion/README.md).
