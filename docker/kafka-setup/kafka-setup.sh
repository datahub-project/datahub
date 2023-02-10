#!/bin/bash

## Exit early if PRECREATION is not needed
if [[ $DATAHUB_PRECREATE_TOPICS == "false" ]]; then
  echo "DATAHUB_PRECREATE_TOPICS=${DATAHUB_PRECREATE_TOPICS}"
  echo "Pre-creation of topics has been turned off, exiting"
  exit 0
fi

. kafka-config.sh

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


############################################################
# Start Topic Creation Logic
############################################################
# make the files
START=$(mktemp -t start-XXXX)
FIFO=$(mktemp -t fifo-XXXX)
FIFO_LOCK=$(mktemp -t lock-XXXX)
START_LOCK=$(mktemp -t lock-XXXX)

## mktemp makes a regular file. Delete that an make a fifo.
rm $FIFO
mkfifo $FIFO
echo $FIFO

## create a trap to cleanup on exit if we fail in the middle.
cleanup() {
  rm $FIFO
  rm $START
  rm $FIFO_LOCK
  rm $START_LOCK
}
trap cleanup 0

# Start worker script
. kafka-topic-workers.sh $START $FIFO $FIFO_LOCK $START_LOCK

## Open the fifo for writing.
exec 3>$FIFO
## Open the start lock for reading
exec 4<$START_LOCK

## Wait for the workers to start
while true; do
  flock 4
  started=$(wc -l $START | cut -d \  -f 1)
  flock -u 4
  if [[ $started -eq $WORKERS ]]; then
    break
  else
    echo waiting, started $started of $WORKERS
  fi
done
exec 4<&-

## utility function to send the jobs to the workers
send() {
  work_id=$1
  topic_args=$2
  echo sending $work_id $topic_args
  echo "$work_id" "$topic_args" 1>&3 ## the fifo is fd 3
}

## Produce the jobs to run.
send "$METADATA_AUDIT_EVENT_NAME" "--partitions $PARTITIONS --topic $METADATA_AUDIT_EVENT_NAME"
send "$METADATA_CHANGE_EVENT_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_EVENT_NAME"
send "$FAILED_METADATA_CHANGE_EVENT_NAME" "--partitions $PARTITIONS --topic $FAILED_METADATA_CHANGE_EVENT_NAME"
send "$METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME"

# Set retention to 90 days
send "$METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME" "--partitions $PARTITIONS --config retention.ms=7776000000 --topic $METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME"
send "$METADATA_CHANGE_PROPOSAL_TOPIC_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_PROPOSAL_TOPIC_NAME"
send "$FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME" "--partitions $PARTITIONS --topic $FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME"
send "$PLATFORM_EVENT_TOPIC_NAME" "--partitions $PARTITIONS --topic $PLATFORM_EVENT_TOPIC_NAME"

# Infinite retention upgrade topic
send "$DATAHUB_UPGRADE_HISTORY_TOPIC_NAME" "--partitions 1 --config retention.ms=-1 --topic $DATAHUB_UPGRADE_HISTORY_TOPIC_NAME"
# Create topic for datahub usage event
if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  send "$DATAHUB_USAGE_EVENT_NAME" "--partitions $PARTITIONS --topic $DATAHUB_USAGE_EVENT_NAME"
fi

## close the filo
exec 3<&-
## disable the cleanup trap
trap '' 0
## It is safe to delete the files because the workers
## already opened them. Thus, only the names are going away
## the actual files will stay there until the workers
## all finish.
cleanup
## now wait for all the workers.
wait

echo "Topic Creation Complete."

############################################################
# End Topic Creation Logic
############################################################

kafka-configs.sh --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --entity-type topics --entity-name _schemas --alter --add-config cleanup.policy=compact
