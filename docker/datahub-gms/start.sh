#!/bin/sh

if [[ -z $ELASTICSEARCH_USERNAME ]]; then
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_HOST
else
  if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD@$ELASTICSEARCH_HOST
  else
    ELASTICSEARCH_HOST_URL=$ELASTICSEARCH_HOST
  fi
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

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent-all.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-gms/scripts/prometheus-config.yaml "
fi

java $JAVA_OPTS $JMX_OPTS \
  $OTEL_AGENT \
  $PROMETHEUS_AGENT \
  -jar /jetty-runner.jar \
  --jar jetty-util.jar \
  --jar jetty-jmx.jar \
  /datahub/datahub-gms/bin/war.war
