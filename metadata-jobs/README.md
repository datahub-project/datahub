# MXE Processing Jobs
DataHub uses Kafka as the pub-sub message queue in the backend. There are 2 Kafka topics used by DataHub which are
`MetadataChangeEvent` and `MetadataAuditEvent`.
* `MetadataChangeEvent:` This message is emitted by any data platform or crawler in which there is a change in the metadata.
* `MetadataAuditEvent:` This message is emitted by [DataHub GMS](../metadata-service/README.md) to notify that metadata change is registered.

To be able to consume from these two topics, there are two Spring
 jobs DataHub uses:
* [MCE Consumer Job](mce-consumer-job): Writes to [DataHub GMS](../metadata-service/README.md)
* [MAE Consumer Job](mae-consumer-job): Writes to [Elasticsearch](../docker/elasticsearch) & [Neo4j](../docker/neo4j)
