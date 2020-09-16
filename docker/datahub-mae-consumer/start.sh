#!/bin/sh

dockerize \
  -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
  -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT \
  -wait http://$NEO4J_HOST \
  -timeout 240s \
  java -jar /datahub/datahub-mae-consumer/bin/mae-consumer-job.jar