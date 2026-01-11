# Defining environment
ARG APP_ENV=prod

FROM alpine:3.22 AS base

# Configurable repositories
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

RUN addgroup -S datahub && adduser -S datahub -G datahub

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# Upgrade Alpine and base packages
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
ENV JMX_VERSION=0.20.0
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add 'c-ares>1.34.5'  --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add 'curl>8.17.0' --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add sqlite libc6-compat \
    && apk --no-cache add snappy=~1.2 --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && apk --no-cache add jattach --repository ${ALPINE_REPO_URL}/edge/community/ \
    && wget ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.15.0/opentelemetry-javaagent.jar -O opentelemetry-javaagent.jar \
    && wget ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar

ENV LD_LIBRARY_PATH="/lib:/lib64"

FROM base AS prod-install

COPY --chown=datahub:datahub --chmod=755 ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/
COPY --chown=datahub:datahub --chmod=755 ./docker/datahub-frontend/start.sh /
COPY --chown=datahub:datahub --chmod=755 ./datahub-frontend/build/stage/main /datahub-frontend/
#Copy all mfe config yamls
COPY --chown=datahub:datahub --chmod=755 ./datahub-frontend/conf/mfe.*.yaml /datahub-frontend/conf/

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134
VOLUME [ "/datahub-frontend" ]

FROM ${APP_ENV}-install AS final
USER datahub

ENV OTEL_EXPORTER_OTLP_MAX_PAYLOAD_SIZE=4194304 \
    OTEL_EXPORTER_OTLP_HTTP_HTTP2_MAX_FRAME_SIZE=16777215 \
    OTEL_EXPORTER_OTLP_HTTP_COMPRESSION=gzip \
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION=gzip

ARG SERVER_PORT=9002
ENV SERVER_PORT=$SERVER_PORT
#ENV gets overwritten by docker-compose file, this sets a fallback value in case not set
ENV MFE_CONFIG_FILE_PATH=/datahub-frontend/conf/mfe.config.dev.yaml
RUN echo $SERVER_PORT
EXPOSE $SERVER_PORT

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$SERVER_PORT${DATAHUB_BASE_PATH}/admin || exit 1
CMD ./start.sh
