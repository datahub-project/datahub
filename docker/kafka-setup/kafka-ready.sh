#!/bin/bash

for i in {1..60}
do
  kafka-broker-api-versions.sh --command-config $CONNECTION_PROPERTIES_PATH --bootstrap-server $KAFKA_BOOTSTRAP_SERVER
  if [ $? -eq 0 ]; then
    break
  fi
  if [ $i -eq 60 ]; then
    echo "Kafka bootstrap server $KAFKA_BOOTSTRAP_SERVER not ready."
    exit 1
  fi
  sleep 5s
done
