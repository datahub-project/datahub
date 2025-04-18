# Kafka, Zookeeper and Schema Registry

DataHub uses Kafka as the pub-sub message queue in the backend.
[Official Confluent Kafka Docker images](https://hub.docker.com/u/confluentinc) found in Docker Hub is used without 
any modification.

## Debugging Kafka
You can install [kafkacat](https://github.com/edenhill/kafkacat) to consume and produce messaged to Kafka topics.
For example, to consume messages on MetadataAuditEvent topic, you can run below command.
```
kafkacat -b localhost:9092 -t MetadataAuditEvent
```
However, `kafkacat` currently doesn't support Avro deserialization at this point, 
but they have an ongoing [work](https://github.com/edenhill/kafkacat/pull/151) for that.