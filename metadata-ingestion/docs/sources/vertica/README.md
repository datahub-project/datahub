## Overview

Vertica can be integrated with DataHub through one or more source modules to ingest metadata into the catalog.

Use this page to select the module that matches your source setup and ingestion use case.

## Concept Mapping

The mapping below provides a platform-level view. Vertica-specific mapping details are provided further below.

| Source Concept | DataHub Concept | Notes |
| --- | --- | --- |
| Platform/account/project scope | Platform Instance, Container | Organizes assets within the platform context. |
| Core technical asset (for example table/view/topic/file) | Dataset | Primary ingested technical asset. |
| Schema fields / columns | SchemaField | Included when schema extraction is supported. |
| Ownership and collaboration principals | CorpUser, CorpGroup | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships | Lineage edges | Available when lineage extraction is supported and enabled. |

Modules on this platform: `vertica`.

### Integration Details

The DataHub Vertica plugin extracts the following:

- Metadata for databases, schemas, views, tables, and projections
- Table level lineage
- Metadata for ML Models

#### Vertica-specific mapping details

This ingestion source maps the following source system concepts to DataHub concepts:

| Source Concept | DataHub Concept                                           | Notes |
| -------------- | --------------------------------------------------------- | ----- |
| `Vertica`      | [Data Platform](../../metamodel/entities/dataPlatform.md) |       |
| Table          | [Dataset](../../metamodel/entities/dataset.md)            |       |
| View           | [Dataset](../../metamodel/entities/dataset.md)            |       |
| Projections    | [Dataset](../../metamodel/entities/dataset.md)            |       |

### Metadata Ingestion Quickstart

For context on getting started with ingestion, check out our [metadata ingestion guide](../../../../metadata-ingestion/README.md).
