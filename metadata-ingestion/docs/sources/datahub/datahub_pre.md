### Overview

Pulls data from two locations:

- **DataHub database**: Versioned aspects
- **DataHub Kafka**: [MCL Log](../../../../docs/what/mxe.md#metadata-change-log-mcl) timeseries aspects

Reads database first, then Kafka. Ingests only data produced before job start (`stop_time` shown in report).

Reads data chronologically: by `createdon` timestamp (database) and Kafka offset (Kafka).

**Required:** Ensure `createdon` column is indexed. Newer databases have the `timeIndex` index by default; older databases require manual creation:

```
CREATE INDEX timeIndex ON metadata_aspect_v2 (createdon);
```

_If you do not have this index, the source may run incredibly slowly and produce
significant database load._

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
