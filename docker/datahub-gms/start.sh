#!/bin/bash

# Add default URI (http) scheme if needed
if ! echo $NEO4J_HOST | grep -q "://" ; then
    NEO4J_HOST="http://$NEO4J_HOST"
fi

if [[ ! -z $ELASTICSEARCH_USERNAME ]] && [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
  ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
fi

# Add default header if needed
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  ELASTICSEARCH_AUTH_HEADER="Accept: */*"
fi

if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
  ELASTICSEARCH_PROTOCOL=https
else
  ELASTICSEARCH_PROTOCOL=http
fi

WAIT_FOR_EBEAN=""
if [[ $SKIP_EBEAN_CHECK != true ]]; then
  if [[ $ENTITY_SERVICE_IMPL == ebean ]] || [[ -z $ENTITY_SERVICE_IMPL ]]; then
    WAIT_FOR_EBEAN=" -wait tcp://$EBEAN_DATASOURCE_HOST "
  fi
fi

WAIT_FOR_CASSANDRA=""
if [[ $ENTITY_SERVICE_IMPL == cassandra ]] && [[ $SKIP_CASSANDRA_CHECK != true ]]; then
  WAIT_FOR_CASSANDRA=" -wait tcp://$CASSANDRA_DATASOURCE_HOST "
fi

WAIT_FOR_KAFKA=""
if [[ $SKIP_KAFKA_CHECK != true ]]; then
  WAIT_FOR_KAFKA=" -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') "
fi

WAIT_FOR_NEO4J=""
if [[ $GRAPH_SERVICE_IMPL != elasticsearch ]] && [[ $SKIP_NEO4J_CHECK != true ]]; then
  WAIT_FOR_NEO4J=" -wait $NEO4J_HOST "
fi

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-gms/scripts/prometheus-config.yaml "
fi

COMMON="
    $WAIT_FOR_EBEAN \
    $WAIT_FOR_CASSANDRA \
    $WAIT_FOR_KAFKA \
    $WAIT_FOR_NEO4J \
    -timeout 240s \
    java $JAVA_OPTS $JMX_OPTS \
    $OTEL_AGENT \
    $PROMETHEUS_AGENT \
    -Dstats=unsecure \
    -jar /datahub/datahub-gms/bin/war.war"

if [[ $SKIP_ELASTICSEARCH_CHECK != true ]]; then
  exec dockerize \
    -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -wait-http-header "$ELASTICSEARCH_AUTH_HEADER" \
    $COMMON
else
  exec dockerize $COMMON
fi
