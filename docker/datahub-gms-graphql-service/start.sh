#!/bin/sh

dockerize \
  -wait tcp://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT \
  -timeout 240s \
  java -jar /datahub/datahub-gms-graphql-service/bin/datahub-gms-graphql-service.jar
