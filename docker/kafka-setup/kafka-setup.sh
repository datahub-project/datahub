#!/bin/bash

## Exit early if PRECREATION is not needed
if [[ $DATAHUB_PRECREATE_TOPICS == "false" ]]; then
  echo "DATAHUB_PRECREATE_TOPICS=${DATAHUB_PRECREATE_TOPICS}"
  echo "Pre-creation of topics has been turned off, exiting"
  exit 0
fi

. kafka-config.sh

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > $CONNECTION_PROPERTIES_PATH

python env_to_properties.py KAFKA_PROPERTIES_ $CONNECTION_PROPERTIES_PATH

# cub kafka-ready -c $CONNECTION_PROPERTIES_PATH -b $KAFKA_BOOTSTRAP_SERVER 1 180
. kafka-ready.sh

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
  topic_config=$3

  echo -e "sending $work_id\n   worker_args: ${topic_args}${DELIMITER}${topic_config}"
  echo "$work_id" "${topic_args}${DELIMITER}${topic_config}" 1>&3 ## the fifo is fd 3
}

## Produce the jobs to run.
send "$METADATA_AUDIT_EVENT_NAME" "--partitions $PARTITIONS --topic $METADATA_AUDIT_EVENT_NAME" \
     "--entity-type topics --entity-name $METADATA_AUDIT_EVENT_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

send "$METADATA_CHANGE_EVENT_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_EVENT_NAME" \
     "--entity-type topics --entity-name $METADATA_CHANGE_EVENT_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"
send "$FAILED_METADATA_CHANGE_EVENT_NAME" "--partitions $PARTITIONS --topic $FAILED_METADATA_CHANGE_EVENT_NAME" \
     "--entity-type topics --entity-name $FAILED_METADATA_CHANGE_EVENT_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

send "$METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME" \
     "--entity-type topics --entity-name $METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

# Set retention to 90 days
send "$METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME" "--partitions $PARTITIONS --config retention.ms=7776000000 --topic $METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME" \
     "--entity-type topics --entity-name $METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

send "$METADATA_CHANGE_PROPOSAL_TOPIC_NAME" "--partitions $PARTITIONS --topic $METADATA_CHANGE_PROPOSAL_TOPIC_NAME" \
     "--entity-type topics --entity-name $METADATA_CHANGE_PROPOSAL_TOPIC_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"
send "$FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME" "--partitions $PARTITIONS --topic $FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME" \
     "--entity-type topics --entity-name $FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

send "$PLATFORM_EVENT_TOPIC_NAME" "--partitions $PARTITIONS --topic $PLATFORM_EVENT_TOPIC_NAME" \
     "--entity-type topics --entity-name $PLATFORM_EVENT_TOPIC_NAME --alter --add-config max.message.bytes=$MAX_MESSAGE_BYTES"

# Infinite retention upgrade topic
  # Make sure the retention.ms config for $DATAHUB_UPGRADE_HISTORY_TOPIC_NAME is configured to infinite
  # Please see the bug report below for details
  # https://github.com/datahub-project/datahub/issues/7882
send "$DATAHUB_UPGRADE_HISTORY_TOPIC_NAME" "--partitions 1 --config retention.ms=-1 --topic $DATAHUB_UPGRADE_HISTORY_TOPIC_NAME" \
     "--entity-type topics --entity-name "$DATAHUB_UPGRADE_HISTORY_TOPIC_NAME" --alter --add-config retention.ms=-1"

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

## If using confluent schema registry as a standalone component, then configure compact cleanup policy.
if [[ $USE_CONFLUENT_SCHEMA_REGISTRY == "TRUE" ]]; then
    kafka-configs.sh --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER \
      --entity-type topics \
      --entity-name _schemas \
      --alter --add-config cleanup.policy=compact
fi
