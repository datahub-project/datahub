#!/bin/sh

#    -wait tcp://GMS_HOST:$GMS_PORT \
dockerize \
  -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
  -timeout 240s \
  java -jar mce-consumer-job.jar