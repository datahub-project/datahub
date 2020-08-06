#!/bin/sh

#    -wait tcp://GMS_HOST:$GMS_PORT \
dockerize \
  -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
  -timeout 240s \
  java -jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar