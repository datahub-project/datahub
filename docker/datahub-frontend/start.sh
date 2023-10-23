#!/bin/sh

echo "Generating user.props file..."
touch /datahub-frontend/conf/user.props
touch /datahub-frontend/conf/tmp.props

echo "admin:${ADMIN_PASSWORD}" >> /datahub-frontend/conf/tmp.props

if [ -n "${CUSTOM_USER_PROPS_FILE}" ]; then
  cat "${CUSTOM_USER_PROPS_FILE}" >> /datahub-frontend/conf/tmp.props
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
export JAVA_OPTS="-Xms512m \
   -Xmx1024m \
   -Dhttp.port=$SERVER_PORT \
   -Dconfig.file=datahub-frontend/conf/application.conf \
   -Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf \
   -Dlogback.configurationFile=datahub-frontend/conf/logback.xml \
   -Dlogback.debug=false \
   -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
   ${PROMETHEUS_AGENT:-} ${OTEL_AGENT:-} \
   ${TRUSTSTORE_FILE:-} ${TRUSTSTORE_TYPE:-} ${TRUSTSTORE_PASSWORD:-} \
   ${HTTP_PROXY:-} ${HTTPS_PROXY:-} ${NO_PROXY:-} \
   -Dpidfile.path=/dev/null"

exec ./datahub-frontend/bin/datahub-frontend
