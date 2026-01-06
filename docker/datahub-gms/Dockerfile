# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

FROM alpine:3.22 AS base

ENV JMX_VERSION=0.20.0

# Re-declaring args from above to make them available in this stage (will inherit default values)
ARG ALPINE_REPO_URL
ARG GITHUB_REPO_URL
ARG MAVEN_CENTRAL_REPO_URL

# Upgrade Alpine and base packages
# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add 'c-ares>1.34.5'  --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add 'curl>8.17.0' --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add bash coreutils gcompat sqlite libc6-compat \
    && apk --no-cache add snappy=~1.2 --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && apk --no-cache add jattach --repository ${ALPINE_REPO_URL}/edge/community/ \
    && wget --no-verbose ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.15.0/opentelemetry-javaagent.jar \
    && wget --no-verbose ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-17-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin

ENV LD_LIBRARY_PATH="/lib:/lib64"

FROM base AS prod-install
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml
COPY docker/datahub-gms/start.sh /datahub/datahub-gms/scripts/start.sh
COPY docker/monitoring/client-prometheus-config.yaml /datahub/datahub-gms/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-gms/scripts/start.sh
COPY metadata-service/war/build/libs/war.war /datahub/datahub-gms/bin/war.war

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final

RUN mkdir -p /etc/datahub/plugins/auth/resources /datahub

RUN addgroup -S datahub && adduser -S datahub -G datahub && chmod g-s /home/datahub
RUN chown -R datahub:datahub /etc/datahub /datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""
ENV OTEL_EXPORTER_OTLP_MAX_PAYLOAD_SIZE=4194304 \
    OTEL_EXPORTER_OTLP_HTTP_HTTP2_MAX_FRAME_SIZE=8388608 \
    OTEL_EXPORTER_OTLP_HTTP_COMPRESSION=gzip \
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION=gzip

EXPOSE 8080

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:8080/${DATAHUB_GMS_BASE_PATH}health || exit 1

CMD /datahub/datahub-gms/scripts/start.sh
