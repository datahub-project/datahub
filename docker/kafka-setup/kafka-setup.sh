#!/bin/bash
: ${PARTITIONS:=1}
: ${REPLICATION_FACTOR:=1}

: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:=PLAINTEXT}

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > ssl.properties
echo "security.protocol=$KAFKA_PROPERTIES_SECURITY_PROTOCOL" >> ssl.properties
echo "ssl.keystore.location=$KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION" >> ssl.properties
echo "ssl.keystore.password=$KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD" >> ssl.properties
echo "ssl.key.password=$KAFKA_PROPERTIES_SSL_KEY_PASSWORD" >> ssl.properties
echo "ssl.truststore.location=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION" >> ssl.properties
echo "ssl.truststore.password=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD" >> ssl.properties
echo "ssl.endpoint.identification.algorithm=$KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" >> ssl.properties

cub kafka-ready -c ssl.properties -b $KAFKA_BOOTSTRAP_SERVER 1 60 && \
kafka-topics --create --if-not-exists --command-config ssl.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_AUDIT_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config ssl.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config ssl.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_EVENT_NAME
