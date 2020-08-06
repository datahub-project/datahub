#!/bin/sh

dockerize \
  -wait tcp://$EBEAN_DATASOURCE_HOST \
  -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
  -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT \
  -wait http://$NEO4J_HOST \
  -timeout 240s \
  java -jar /jetty-runner.jar /datahub/datahub-gms/bin/war.war