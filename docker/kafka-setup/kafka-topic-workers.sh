#!/usr/bin/env bash

. kafka-config.sh

START=$1
FIFO=$2
FIFO_LOCK=$3
START_LOCK=$4

## this is the "job" function which is does whatever work
## the queue workers are supposed to be doing
job() {
  i=$1
  worker_args=$2
  topic_args=$(echo $worker_args | cut -d "$DELIMITER" -f 1)
  topic_config=$(echo $worker_args | cut -d "$DELIMITER" -f 2)

  echo "   $i: kafka-topics.sh --create --if-not-exist $topic_args"
  kafka-topics.sh --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER \
     --replication-factor $REPLICATION_FACTOR \
     $topic_args
  if [[ ! -z "$topic_config" ]]; then
    echo "   $i: kafka-configs.sh $topic_config"
    kafka-configs.sh --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER $topic_config
  fi
}

## This is the worker to read from the queue.
work() {
  ID=$1
  ## first open the fifo and locks for reading.
  exec 3<$FIFO
  exec 4<$FIFO_LOCK
  exec 5<$START_LOCK

  ## signal the worker has started.
  flock 5                 # obtain the start lock
  echo $ID >> $START      # put my worker ID in the start file
  flock -u 5              # release the start lock
  exec 5<&-               # close the start lock file
  echo worker $ID started

  while true; do
    ## try to read the queue
    flock 4                      # obtain the fifo lock
    read -su 3 work_id work_item # read into work_id and work_item
    read_status=$?               # save the exit status of read
    flock -u 4                   # release the fifo lock

    ## check the line read.
    if [[ $read_status -eq 0 ]]; then
      ## If read gives an exit code of 0 the read succeeded.
      # got a work item. do the work
      echo $ID got work_id=$work_id topic_args=$work_item
      ## Run the job in a subshell. That way any exit calls do not kill
      ## the worker process.
      ( job "$work_id" "$work_item" )
    else
      ## Any other exit code indicates an EOF.
      break
    fi
  done
  # clean up the fd(s)
  exec 3<&-
  exec 4<&-
  echo $ID "done working"
}

## Start the workers.
for ((i=1;i<=$WORKERS;i++)); do
  echo will start $i
  work $i &
done

