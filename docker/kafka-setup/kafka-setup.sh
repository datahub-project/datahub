#!/bin/bash
: ${PARTITIONS:=1}
: ${REPLICATION_FACTOR:=1}

: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:=PLAINTEXT}

: ${DATAHUB_ANALYTICS_ENABLED:=true}

CONNECTION_PROPERTIES_PATH=/tmp/connection.properties

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > $CONNECTION_PROPERTIES_PATH
echo "security.protocol=$KAFKA_PROPERTIES_SECURITY_PROTOCOL" >> $CONNECTION_PROPERTIES_PATH

if [[ $KAFKA_PROPERTIES_SECURITY_PROTOCOL == "SSL" ]]; then
    if [[ -n $KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION ]]; then
        echo "ssl.keystore.location=$KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION" >> $CONNECTION_PROPERTIES_PATH
        echo "ssl.keystore.password=$KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
        echo "ssl.key.password=$KAFKA_PROPERTIES_SSL_KEY_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
        if [[ -n $KAFKA_PROPERTIES_SSL_KEYSTORE_TYPE ]]; then
            echo "ssl.keystore.type=$KAFKA_PROPERTIES_SSL_KEYSTORE_TYPE" >> $CONNECTION_PROPERTIES_PATH
        fi
    fi
    if [[ -n $KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION ]]; then
        echo "ssl.truststore.location=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION" >> $CONNECTION_PROPERTIES_PATH
        echo "ssl.truststore.password=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
        if [[ -n $KAFKA_PROPERTIES_SSL_TRUSTSTORE_TYPE ]]; then
            echo "ssl.truststore.type=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_TYPE" >> $CONNECTION_PROPERTIES_PATH
        fi
    fi
    echo "ssl.endpoint.identification.algorithm=$KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" >> $CONNECTION_PROPERTIES_PATH
fi

cub kafka-ready -c $CONNECTION_PROPERTIES_PATH -b $KAFKA_BOOTSTRAP_SERVER 1 60
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_AUDIT_EVENT_NAME
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_EVENT_NAME
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_EVENT_NAME
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_LOG_VERSIONED_TOPIC

# Set retention to 90 days
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --config retention.ms=7776000000 --topic $METADATA_CHANGE_LOG_TIMESERIES_TOPIC
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_PROPOSAL_TOPIC
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_PROPOSAL_TOPIC
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $PLATFORM_EVENT_TOPIC_NAME

# Create topic for datahub usage event
if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $DATAHUB_USAGE_EVENT_NAME
fi
