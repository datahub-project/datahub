# This "container" is a workaround to pre-create topics
FROM confluentinc/cp-kafka:5.4.0

ENV METADATA_AUDIT_EVENT_NAME = "MetadataAuditEvent_v4"
ENV METADATA_CHANGE_EVENT_NAME = "MetadataChangeEvent_v4"
ENV FAILED_METADATA_CHANGE_EVENT_NAME = "FailedMetadataChangeEvent_v4"

CMD echo Waiting for Kafka to be ready... && \
    cub kafka-ready -b $KAFKA_BOOTSTRAP_SERVER 1 60 && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic $METADATA_AUDIT_EVENT_NAME && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic $METADATA_CHANGE_EVENT_NAME && \
    kafka-topics --create --if-not-exists --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions 1 --replication-factor 1 --topic $FAILED_METADATA_CHANGE_EVENT_NAME
