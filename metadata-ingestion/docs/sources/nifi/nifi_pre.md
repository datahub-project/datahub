### Overview

The `nifi` module ingests metadata from Nifi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

### Concept Mapping

| Source Concept                    | DataHub Concept                                           | Notes                   |
| --------------------------------- | --------------------------------------------------------- | ----------------------- |
| `"Nifi"`                          | [Data Platform](../../metamodel/entities/dataPlatform.md) |                         |
| Nifi flow                         | [Data Flow](../../metamodel/entities/dataFlow.md)         |                         |
| Nifi Ingress / Egress Processor   | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Remote Port                  | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Port with remote connections | [Dataset](../../metamodel/entities/dataset.md)            |                         |
| Nifi Process Group                | [Container](../../metamodel/entities/container.md)        | Subtype `Process Group` |

### Caveats

- Lineage extraction analyzes provenance events. Verify your NiFi provenance retention period and run ingestion frequently enough to capture events before they expire.

- Limited ingress/egress processors are supported
  - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
  - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`
