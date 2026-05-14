#!/bin/sh
set -u

# Use absolute paths so -Dconfig.file and logback work when CWD is not / (e.g. some K8s/compose
# setups set workdir to the app root; relative datahub-frontend/conf/... would not exist).
DATAHUB_HOME="${DATAHUB_HOME:-/datahub-frontend}"

PROMETHEUS_AGENT=""
if [[ ${ENABLE_PROMETHEUS:-false} == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:${DATAHUB_HOME}/client-prometheus-config.yaml"
fi

export MANAGEMENT_SERVER_PORT="${MANAGEMENT_SERVER_PORT:-4319}"

OTEL_AGENT=""
if [[ ${ENABLE_OTEL:-false} == true ]]; then
  OTEL_AGENT="-javaagent:/opentelemetry-javaagent.jar"
fi

TRUSTSTORE_FILE=""
if [[ ! -z ${SSL_TRUSTSTORE_FILE:-} ]]; then
  TRUSTSTORE_FILE="-Djavax.net.ssl.trustStore=$SSL_TRUSTSTORE_FILE"
fi

TRUSTSTORE_TYPE=""
if [[ ! -z ${SSL_TRUSTSTORE_TYPE:-} ]]; then
  TRUSTSTORE_TYPE="-Djavax.net.ssl.trustStoreType=$SSL_TRUSTSTORE_TYPE"
fi

TRUSTSTORE_PASSWORD=""
if [[ ! -z ${SSL_TRUSTSTORE_PASSWORD:-} ]]; then
  TRUSTSTORE_PASSWORD="-Djavax.net.ssl.trustStorePassword=$SSL_TRUSTSTORE_PASSWORD"
fi

HTTP_PROXY=""
if [[ ! -z ${HTTP_PROXY_HOST:-} ]] && [[ ! -z ${HTTP_PROXY_PORT:-} ]]; then
  HTTP_PROXY="-Dhttp.proxyHost=$HTTP_PROXY_HOST -Dhttp.proxyPort=$HTTP_PROXY_PORT"
fi

HTTPS_PROXY=""
if [[ ! -z ${HTTPS_PROXY_HOST:-} ]] && [[ ! -z ${HTTPS_PROXY_PORT:-} ]]; then
  HTTPS_PROXY="-Dhttps.proxyHost=$HTTPS_PROXY_HOST -Dhttps.proxyPort=$HTTPS_PROXY_PORT"
fi

NO_PROXY=""
if [[ ! -z ${HTTP_NON_PROXY_HOSTS:-} ]]; then
  NO_PROXY="-Dhttp.nonProxyHosts='$HTTP_NON_PROXY_HOSTS'"
fi

# make sure there is no whitespace at the beginning and the end of
# this string
export JAVA_OPTS="${JAVA_MEMORY_OPTS:-"-Xms512m -Xmx1024m"} \
   -Dhttp.port=$SERVER_PORT \
   -Dconfig.file=${DATAHUB_HOME}/conf/application.conf \
   -Djava.security.auth.login.config=${DATAHUB_HOME}/conf/jaas.conf \
   -Dlogback.configurationFile=${DATAHUB_HOME}/conf/logback.xml \
   -Dlogback.debug=false \
   ${PROMETHEUS_AGENT:-} ${OTEL_AGENT:-} \
   ${TRUSTSTORE_FILE:-} ${TRUSTSTORE_TYPE:-} ${TRUSTSTORE_PASSWORD:-} \
   ${HTTP_PROXY:-} ${HTTPS_PROXY:-} ${NO_PROXY:-} \
   -Dpidfile.path=/dev/null"

exec "${DATAHUB_HOME}/bin/datahub-frontend"

