### Overview

Migrate data from one DataHub instance to another.

Pulls data from two locations:

- **DataHub database**: Versioned aspects
- **DataHub Kafka**: [MCL Log](../../../../docs/what/mxe.md#metadata-change-log-mcl) timeseries aspects

All data is first read from the database, before timeseries data is ingested from kafka. To prevent this source from potentially running forever, it will not ingest data produced after the datahub_source ingestion job is started. This stop_time is reflected in the report.

Data from the database and kafka are read in chronological order, specifically by the createdon timestamp in the database and by kafka offset per partition. In order to properly read from the database, please ensure that the createdon column is indexed. Newly created databases should have this index, named timeIndex, by default, but older ones you may have to create yourself, with the statement:

```
CREATE INDEX timeIndex ON metadata_aspect_v2 (createdon);
```

:::warning Indexing createdon

If you do not have this index, the source may run incredibly slowly and produce significant database load.
:::

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

Requires direct access to the database, kafka broker, and kafka schema registry
of the source DataHub instance.
