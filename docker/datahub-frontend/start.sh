#!/bin/sh

echo "Generating user.props file..."
mkdir -p /datahub-frontend/conf/
touch /datahub-frontend/conf/user.props
touch /datahub-frontend/conf/tmp.props

echo "admin:${ADMIN_PASSWORD}" >> /datahub-frontend/conf/tmp.props

if [ -n "${CUSTOM_USER_PROPS_FILE}" ]; then
  cat "${CUSTOM_USER_PROPS_FILE}" >> /datahub-frontend/conf/tmp.props
fi

PROMETHEUS_AGENT=""
if [[ ${ENABLE_PROMETHEUS:-false} == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub-frontend/client-prometheus-config.yaml"
fi

OTEL_AGENT=""
if [[ ${ENABLE_OTEL:-false} == true ]]; then
  OTEL_AGENT="-javaagent:/opentelemetry-javaagent.jar"
fi

# Remove empty newlines, if there are any.
sed '/^[[:space:]]*$/d' /datahub-frontend/conf/tmp.props > /datahub-frontend/conf/user.props
rm /datahub-frontend/conf/tmp.props

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
   -Dconfig.file=datahub-frontend/conf/application.conf \
   -Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf \
   -Dlogback.configurationFile=datahub-frontend/conf/logback.xml \
   -Dlogback.debug=false \
   ${PROMETHEUS_AGENT:-} ${OTEL_AGENT:-} \
   ${TRUSTSTORE_FILE:-} ${TRUSTSTORE_TYPE:-} ${TRUSTSTORE_PASSWORD:-} \
   ${HTTP_PROXY:-} ${HTTPS_PROXY:-} ${NO_PROXY:-} \
   -Dpidfile.path=/dev/null"

JAR_PATH="/datahub-frontend/lib/datahub-web-react-datahub-web-react-assets.jar"
if [ "${METICULOUS_ENABLED:-false}" = "true" ]; then
  echo "Meticulous: Swapping index.html with index.dev.html in JAR..."
  if [ -f "$JAR_PATH" ]; then
    mkdir -p /tmp
    cd /tmp
    unzip "$JAR_PATH" public/index.dev.html
    cp public/index.dev.html public/index.html
    zip -u "$JAR_PATH" public/index.html
    cd ..
    echo "Meticulous: File swap complete"
  else
    echo "Meticulous: JAR not found at $JAR_PATH"
  fi
fi

exec ./datahub-frontend/bin/datahub-frontend
