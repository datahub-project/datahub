#!/bin/bash
set -x
# Add default URI (http) scheme to NEO4J_HOST if missing
if [[ -n "$NEO4J_HOST" && $NEO4J_HOST != *"://"* ]] ; then
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

# Add elasticsearch protocol
if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
  ELASTICSEARCH_PROTOCOL=https
else
  ELASTICSEARCH_PROTOCOL=http
fi

WAIT_FOR_EBEAN=""
if [[ $SKIP_EBEAN_CHECK != true ]]; then
  WAIT_FOR_EBEAN=" -wait tcp://$EBEAN_DATASOURCE_HOST "
fi

WAIT_FOR_KAFKA=""
if [[ $SKIP_KAFKA_CHECK != true ]]; then
  WAIT_FOR_KAFKA=" -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') "
fi

# Add dependency to graph service if needed
WAIT_FOR_GRAPH_SERVICE=""
if [[ $GRAPH_SERVICE_IMPL == neo4j ]] && [[ $SKIP_NEO4J_CHECK != true ]]; then
  if [[ -z "$NEO4J_HOST" ]]; then
    echo "GRAPH_SERVICE_IMPL set to neo4j but no NEO4J_HOST set"
    exit 1
  fi
  WAIT_FOR_GRAPH_SERVICE=" -wait $NEO4J_HOST "
elif [[ $GRAPH_SERVICE_IMPL == dgraph ]] && [[ $SKIP_DGRAPH_CHECK != true ]]; then
  if [[ -z "$DGRAPH_HOST" ]]; then
    echo "GRAPH_SERVICE_IMPL set to dgraph but no DGRAPH_HOST set"
    exit 1
  fi
  if [[ -n "$DGRAPH_HOST" && $DGRAPH_HOST != *":"* ]] ; then
    DGRAPH_HOST="$DGRAPH_HOST:9080"
  fi
  WAIT_FOR_GRAPH_SERVICE=" -wait tcp://$DGRAPH_HOST "
fi

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent-all.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-gms/scripts/prometheus-config.yaml "
fi

COMMON="
    $WAIT_FOR_EBEAN \
    $WAIT_FOR_KAFKA \
    $WAIT_FOR_GRAPH_SERVICE \
    -timeout 240s \
    java $JAVA_OPTS $JMX_OPTS \
    $OTEL_AGENT \
    $PROMETHEUS_AGENT \
    -jar /jetty-runner.jar \
    --jar jetty-util.jar \
    --jar jetty-jmx.jar \
    /datahub/datahub-gms/bin/war.war"

if [[ $SKIP_ELASTICSEARCH_CHECK != true ]]; then
  dockerize \
    -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -wait-http-header "$ELASTICSEARCH_AUTH_HEADER" \
    $COMMON
else
  dockerize $COMMON
fi
