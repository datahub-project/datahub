# Defining environment
ARG APP_ENV=prod

ARG BASE_IMAGE=cgr.dev/chainguard/wolfi-base:latest
FROM ${BASE_IMAGE} AS base

ARG APK_REPOSITORY_URL=https://apk.cgr.dev/chainguard
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

ENV JMX_VERSION=1.0.1

RUN printf '%s\n' "${APK_REPOSITORY_URL}" > /etc/apk/repositories \
    && apk update --no-cache \
    && apk upgrade --no-cache

COPY docker/snippets/setup_java_runtime.sh /tmp/setup_java_runtime.sh
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    chmod +x /tmp/setup_java_runtime.sh \
    && INSTALL_JATTACH=1 OTEL_JMX_DIR=/ /tmp/setup_java_runtime.sh \
    && rm -f /tmp/setup_java_runtime.sh \
    && addgroup -S datahub && adduser -S datahub -G datahub

FROM base AS prod-install

COPY --chown=datahub:datahub --chmod=755 ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/
COPY --chown=datahub:datahub --chmod=755 ./docker/datahub-frontend/start.sh /
COPY --chown=datahub:datahub --chmod=755 ./datahub-frontend/build/stage/main /datahub-frontend/
# Copy all mfe config yamls
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
# ENV gets overwritten by docker-compose file, this sets a fallback value in case not set
ENV MFE_CONFIG_FILE_PATH=/datahub-frontend/conf/mfe.config.dev.yaml
RUN echo $SERVER_PORT
EXPOSE $SERVER_PORT

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$SERVER_PORT${DATAHUB_BASE_PATH}/admin || exit 1
CMD ./start.sh
