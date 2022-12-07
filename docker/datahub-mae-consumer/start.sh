#!/bin/bash
set -euo pipefail

# Add default URI (http) scheme if needed
if [[ -n ${NEO4J_HOST:-} ]] && [[ ${NEO4J_HOST} != *"://"* ]]; then
  NEO4J_HOST="http://$NEO4J_HOST"
fi

if [[ -n ${ELASTICSEARCH_USERNAME:-} ]] && [[ -z ${ELASTICSEARCH_AUTH_HEADER:-} ]]; then
  AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
  ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
fi

# Add default header if needed
: "${ELASTICSEARCH_AUTH_HEADER="Accept: */*"}"

if [[ ${ELASTICSEARCH_USE_SSL:-false} == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi

dockerize_args=("-timeout" "240s")
if [[ ${SKIP_KAFKA_CHECK:-false} != true ]]; then
  IFS=',' read -ra KAFKAS <<< "$KAFKA_BOOTSTRAP_SERVER"
  for i in "${KAFKAS[@]}"; do
    dockerize_args+=("-wait" "tcp://$i")
  done
fi
if [[ ${SKIP_ELASTICSEARCH_CHECK:-false} != true ]]; then
  dockerize_args+=("-wait" "$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT" "-wait-http-header" "$ELASTICSEARCH_AUTH_HEADER")
fi
if [[ ${GRAPH_SERVICE_IMPL:-} != elasticsearch ]] && [[ ${SKIP_NEO4J_CHECK:-false} != true ]]; then
  dockerize_args+=("-wait" "$NEO4J_HOST")
fi

JAVA_TOOL_OPTIONS="${JDK_JAVA_OPTIONS:-}${JAVA_OPTS:+ $JAVA_OPTS}${JMX_OPTS:+ $JMX_OPTS}"
if [[ ${ENABLE_OTEL:-false} == true ]]; then
  JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -javaagent:opentelemetry-javaagent.jar"
fi
if [[ ${ENABLE_PROMETHEUS:-false} == true ]]; then
  JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-mae-consumer/scripts/prometheus-config.yaml"
fi

export JAVA_TOOL_OPTIONS
exec dockerize "${dockerize_args[@]}" java -jar /datahub/datahub-mae-consumer/bin/mae-consumer-job.jar
