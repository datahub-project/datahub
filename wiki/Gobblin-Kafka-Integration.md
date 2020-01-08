> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

WhereHows is listening to several Kafka topics for Gobblin jobs emitted messages. Those message contains instant information about the ETL jobs and the impacted datasets. For example, when a dataset is updated or when a snapshot is published.

WhereHows is currently listening to three GobblinTrackingEvent related topics. Each Gobblin Kafka event has similar schema:
```json
{
  "type": "record",
  "name": "GobblinTrackingEvent",
  "namespace": "gobblin.metrics",
  "fields": [
    {
      "name": "timestamp",
      "type": "long",
      "doc": "Time at which event was created.",
      "default": 0
    },
    {
      "name": "namespace",
      "type": [
        "string",
        "null"
      ],
      "doc": "Namespace used for filtering of events."
    },
    {
      "name": "name",
      "type": "string",
      "doc": "Event name."
    },
    {
      "name": "metadata",
      "type": {
        "type": "map",
        "values": "string"
      },
      "doc": "Event metadata.",
      "default": {}
    }
  ]
}
```

The name and namespace of the event defines the scope of the event and the content of the event is in the field 'metadata' which is a Map<String, String>. Different Gobblin events may have different Keys in the 'metadata' Map.

The processed information and records are stored in corresponding staging tables first. The table DDL is in [kafka_tracking.sql](../wherehows-data-model/DDL/ETL_DDL/kafka_tracking.sql)

# GobblinTrackingEvent

WhereHows is tracking the GobblinTrackingEvent events with name='CompactionCompleted'.

An example metadata field has following format:
```json
{  
  "azkabanJobId":"tracking-hourly-compaction",
  "partition":"2016/07/13/00",
  "metricContextID":"dafadfadsfasdfas",
  "recordCount":"157088",
  "dedupeStatus":"DEDUPED",
  "clusterIdentifier":"tarock",
  "azkabanProjectName":"Gobblin-Kafka",
  "datasetUrn":"/data/tracking/abc",
  "azkabanExecId":"1512",
  "metricContextName":"GobblinKafkaTrackingHourlyCompaction"
}
```

This events are processed through [GobblinTrackingCompactionProcessor.java](../wherehows-etl/src/main/java/metadata/etl/kafka/GobblinTrackingCompactionProcessor.java) and the result is a 'GobblinTrackingCompactionRecord' and stored in the 'stg_kafka_gobblin_compaction' table.


# GobblinTrackingEvent_lumos

In GobblinTrackingEvent_lumos events we are looking for name='DeltaPublished' or name='SnapshotPublished'.

An example metadata field has following format:
```json
{
  "azkabanJobId": "lumos2-virtual-snapshot-builder",
  "Table": "GroupMemberCounts",
  "Dropdate": "1467526698875",
  "originTimestamp": "1467000000000000",
  "recordCount": "570",
  "VSBMapperInputCount": "0",
  "clusterIdentifier": "tarock",
  "azkabanFlowId": "lumos2-incremental",
  "metricContextName": "Lumos",
  "TargetDirectory": "/data/abc/1467526698875-PT-1561",
  "JobType": "VirtualSnapshotBuilder",
  "partition": "1467507599999-scn-570",
  "metricContextID": "89902742",
  "Database": "Leap",
  "DatasourceColo": "prod-lva1",
  "azkabanProjectName": "Lumos_V2_Prod",
  "datasetUrn": "/data/abc",
  "IsFull": "false",
  "azkabanExecId": "1470416"
}
```

This events are processed through [GobblinTrackingLumosProcessor.java](../wherehows-etl/src/main/java/metadata/etl/kafka/GobblinTrackingLumosProcessor.java) and the result is a 'GobblinTrackingLumosRecord' and stored in the 'stg_kafka_gobblin_lumos' table.


# GobblinTrackingEvent_distcp_ng

WhereHows is tracking the GobblinTrackingEvent_distcp_ng events with name='DatasetPublished'.

An example metadata field has following format:
```json
{
  "jobName": "DistcpHiveTracking",
  "azkabanJobId": "data-tracking-hive-copy",
  "upstreamTimestamp": "1467526698875",
  "originTimestamp": "1468135417671",
  "clusterIdentifier": "ltx1",
  "azkabanFlowId": "data-tracking-hive-copy",
  "metricContextName": "gobblin.data.management.copy.publisher.CopyDataPublisher",
  "jobId": "job_DistcpHiveTracking",
  "partition": "[\"2016-07-10-00\"]",
  "metricContextID": "24706980",
  "azkabanProjectName": "distcp-ng",
  "datasetUrn": "tracking@abc",
  "class": "gobblin.data.management.copy.publisher.CopyDataPublisher",
  "azkabanExecId": "679377"
}
```

This events are processed through [GobblinTrackingDistcpNgProcessor.java](../wherehows-etl/src/main/java/metadata/etl/kafka/GobblinTrackingDistcpNgProcessor.java) and the result is a 'GobblinTrackingDistcpNgRecord' and stored in the 'stg_kafka_gobblin_distcp' table.
