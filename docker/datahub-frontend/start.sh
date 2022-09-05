#!/bin/sh
set -u

PROMETHEUS_AGENT=""
if [[ ${ENABLE_PROMETHEUS:-false} == true ]]; then
  PROMETHEUS_AGENT="-javaagent:/jmx_prometheus_javaagent.jar=4318:/datahub-frontend/scripts/prometheus-config.yaml "
fi

OTEL_AGENT=""
if [[ ${ENABLE_OTEL:-false} == true ]]; then
  OTEL_AGENT="-javaagent:/opentelemetry-javaagent-all.jar "
fi

export JAVA_OPTS=" -Xms512m \
   -Xmx1024m \
   -Dhttp.port=$SERVER_PORT \
   -Dconfig.file=datahub-frontend/conf/application.conf \
   -Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf \
   -Dlogback.configurationFile=datahub-frontend/conf/logback.xml \
   -Dlogback.debug=false \
   -Dpidfile.path=/dev/null \ ${PROMETHEUS_AGENT:-} ${OTEL_AGENT:-} "

exec ./datahub-frontend/bin/datahub-frontend

