#!/bin/sh

dockerize \
  -wait http://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT \
  -timeout 240s \
  java -jar /datahub/metadata-graphql-api/bin/metadata-graphql-api.jar
