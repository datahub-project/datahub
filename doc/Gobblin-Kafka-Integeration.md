WhereHows is listening to several Kafka topics for Gobblin jobs emitted messages. Those message contains instant information about the ETL jobs and the impacted datasets. For example, when a dataset is updated or when a snapshot is published. 

# GobblinTrackingCompaction events

We are tracking the GobblinTrackingCompaction events with name='CompactionCompleted' in auditHeader. The message information is contained in the "metadata" field which is a Map<String, String>. 

An example metadata field has following format:
`{"azkabanJobId":"tracking-hourly-compaction","partition":"2016/07/13/00","metricContextID":"dafadfadsfasdfas","recordCount":"157088","dedupeStatus":"DEDUPED","clusterIdentifier":"xxxxxx","azkabanProjectName":"Gobblin-Kafka","datasetUrn":"/data/tracking/abc","azkabanExecId":"1512","metricContextName":"GobblinKafkaTrackingHourlyCompaction"}`

This events are processed through [GobblinTrackingCompactionProcessor.java](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/java/metadata/etl/kafka/GobblinTrackingCompactionProcessor.java) and the result is a 'GobblinTrackingCompactionRecord' and stored in the 'stg_kafka_gobblin_compaction' table.

# GobblinTrackingCompaction_lumos events

We are tracking the GobblinTrackingCompaction events with name='CompactionCompleted' in auditHeader. The message information is contained in the "metadata" field which is a Map<String, String>. 

An example metadata field has following format:
`{"azkabanJobId":"tracking-hourly-compaction","partition":"2016/07/13/00","metricContextID":"dafadfadsfasdfas","recordCount":"157088","dedupeStatus":"DEDUPED","clusterIdentifier":"xxxxxx","azkabanProjectName":"Gobblin-Kafka","datasetUrn":"/data/tracking/abc","azkabanExecId":"1512","metricContextName":"GobblinKafkaTrackingHourlyCompaction"}`

This events are processed through [GobblinTrackingCompactionProcessor.java](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/java/metadata/etl/kafka/GobblinTrackingCompactionProcessor.java) and the result is a 'GobblinTrackingCompactionRecord' and stored in the 'stg_kafka_gobblin_compaction' table.

# GobblinTrackingCompaction_distcp_ng events

We are tracking the GobblinTrackingCompaction events with name='CompactionCompleted' in auditHeader. The message information is contained in the "metadata" field which is a Map<String, String>. 

An example metadata field has following format:
`{"azkabanJobId":"tracking-hourly-compaction","partition":"2016/07/13/00","metricContextID":"dafadfadsfasdfas","recordCount":"157088","dedupeStatus":"DEDUPED","clusterIdentifier":"xxxxxx","azkabanProjectName":"Gobblin-Kafka","datasetUrn":"/data/tracking/abc","azkabanExecId":"1512","metricContextName":"GobblinKafkaTrackingHourlyCompaction"}`

This events are processed through [GobblinTrackingCompactionProcessor.java](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/java/metadata/etl/kafka/GobblinTrackingCompactionProcessor.java) and the result is a 'GobblinTrackingCompactionRecord' and stored in the 'stg_kafka_gobblin_compaction' table.

