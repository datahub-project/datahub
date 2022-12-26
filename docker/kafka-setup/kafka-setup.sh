#!/bin/bash
: ${PARTITIONS:=1}
: ${REPLICATION_FACTOR:=1}

: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:=PLAINTEXT}

: ${DATAHUB_ANALYTICS_ENABLED:=true}

CONNECTION_PROPERTIES_PATH=/tmp/connection.properties

function wait_ex {
    # this waits for all jobs and returns the exit code of the last failing job
    ecode=0
    while true; do
        [ -z "$(jobs)" ] && break
        wait -n
        err="$?"
        [ "$err" != "0" ] && ecode="$err"
    done
    return $ecode
}

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > $CONNECTION_PROPERTIES_PATH
echo "security.protocol=$KAFKA_PROPERTIES_SECURITY_PROTOCOL" >> $CONNECTION_PROPERTIES_PATH

## Add support for SASL_PLAINTEXT
if [[ $KAFKA_PROPERTIES_SECURITY_PROTOCOL == "SASL_PLAINTEXT" ]]; then
    echo "sasl.jaas.config=$KAFKA_PROPERTIES_SASL_JAAS_CONFIG" >> $CONNECTION_PROPERTIES_PATH
    echo "sasl.kerberos.service.name=$KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME" >> $CONNECTION_PROPERTIES_PATH
fi

## Add support for SASL_SSL
if [[ $KAFKA_PROPERTIES_SECURITY_PROTOCOL == "SASL_SSL" ]]; then
    echo "sasl.jaas.config=$KAFKA_PROPERTIES_SASL_JAAS_CONFIG" >> $CONNECTION_PROPERTIES_PATH
    echo "sasl.mechanism=$KAFKA_PROPERTIES_SASL_MECHANISM" >> $CONNECTION_PROPERTIES_PATH
fi

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

# Add support for SASL_CLIENT_CALLBACK_HANDLER_CLASS
if [[ -n "$KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS" ]]; then
    echo "sasl.client.callback.handler.class=$KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS" >> $CONNECTION_PROPERTIES_PATH
fi

cub kafka-ready -c $CONNECTION_PROPERTIES_PATH -b $KAFKA_BOOTSTRAP_SERVER 1 180
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_AUDIT_EVENT_NAME &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_EVENT_NAME &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_EVENT_NAME &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_LOG_VERSIONED_TOPIC &

# Set retention to 90 days
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --config retention.ms=7776000000 --topic $METADATA_CHANGE_LOG_TIMESERIES_TOPIC &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_PROPOSAL_TOPIC &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_PROPOSAL_TOPIC &
kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $PLATFORM_EVENT_TOPIC_NAME &

# Create topic for datahub usage event
if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $DATAHUB_USAGE_EVENT_NAME &
fi

echo "Waiting for topic creation."
result=$(wait_ex)
rc=$?
if [ $rc -ne 0 ]; then exit $rc; fi
echo "Finished topic creation."

kafka-configs.sh --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --entity-type topics --entity-name _schemas --alter --add-config cleanup.policy=compact
