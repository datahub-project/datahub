#!/bin/bash
: ${PARTITIONS:=1}
: ${REPLICATION_FACTOR:=1}

: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:=PLAINTEXT}

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > connection.properties
echo "security.protocol=$KAFKA_PROPERTIES_SECURITY_PROTOCOL" >> connection.properties

if [[ $KAFKA_PROPERTIES_SECURITY_PROTOCOL == "SSL" ]]; then
    echo "ssl.keystore.location=$KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION" >> connection.properties
    echo "ssl.keystore.password=$KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD" >> connection.properties
    echo "ssl.key.password=$KAFKA_PROPERTIES_SSL_KEY_PASSWORD" >> connection.properties
    echo "ssl.truststore.location=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION" >> connection.properties
    echo "ssl.truststore.password=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD" >> connection.properties
    echo "ssl.endpoint.identification.algorithm=$KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" >> connection.properties
fi

cub kafka-ready -c connection.properties -b $KAFKA_BOOTSTRAP_SERVER 1 60 && \
kafka-topics --create --if-not-exists --command-config connection.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_AUDIT_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config connection.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config connection.properties --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_EVENT_NAME
