# MXE Consumer Jobs
Data Hub uses Kafka as the pub-sub message queue in the backend. There are 2 Kafka topics used by Data Hub which are 
`MetadataChangeEvent` and `MetadataAuditEvent`.
* `MetadataChangeEvent:` This message is emitted by any data platform or crawler in which there is a change in the metadata.
* `MetadataAuditEvent:` This message is emitted by [Data Hub GMS](../gms) to notify that metadata change is registered.

To be able to consume from these two topics, there are two [Kafka Streams](https://kafka.apache.org/documentation/streams/)
 jobs Data Hub uses:
* [MCE Consumer Job](mce-consumer-job): Writes to [Data Hub GMS](../gms)
* [MAE Consumer Job](mae-consumer-job): Writes to [Elasticsearch](../docker/elasticsearch)