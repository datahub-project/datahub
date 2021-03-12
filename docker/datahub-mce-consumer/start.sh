#!/bin/sh

#    -wait tcp://GMS_HOST:$GMS_PORT \
dockerize \
  -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') \
  -timeout 240s \
  java -jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar