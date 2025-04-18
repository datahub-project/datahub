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
- This plugin extracts the lineage information between external datasets and ingress/egress processors by analyzing provenance events. Please check your Nifi configuration to confirm max rentention period of provenance events and make sure that ingestion runs frequent enough to read provenance events before they are disappear.

- Limited ingress/egress processors are supported
    - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
    - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`