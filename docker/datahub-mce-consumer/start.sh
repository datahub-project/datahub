#!/bin/sh

dockerize \
  -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') \
  -timeout 240s \
  java $JAVA_OPTS $JMX_OPTS -jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar