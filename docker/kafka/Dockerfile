# This "container" is a workaround to pre-create topics
FROM confluentinc/cp-kafka:5.4.0

CMD echo Waiting for Kafka to be ready... && \
    cub kafka-ready -b $KAFKA_BOOTSTRAP_SERVER 1 60 && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic MetadataAuditEvent_v4 && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic MetadataChangeEvent_v4 && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic FailedMetadataChangeEvent_v4
