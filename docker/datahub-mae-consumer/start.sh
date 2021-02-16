#!/bin/sh

# Add default URI (http) scheme if needed
if [[ "$NEO4J_HOST" != *"://"* ]]; then
    NEO4J_HOST="http://$NEO4J_HOST"
fi

dockerize \
  -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
  -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT \
  -wait $NEO4J_HOST \
  -timeout 240s \
  java -jar /datahub/datahub-mae-consumer/bin/mae-consumer-job.jar