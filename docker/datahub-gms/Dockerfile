# Defining environment
ARG APP_ENV=prod

ARG BASE_IMAGE=cgr.dev/chainguard/wolfi-base:latest
FROM ${BASE_IMAGE} AS base

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
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
    && rm -f /tmp/setup_java_runtime.sh

COPY docker/snippets/wait_for_deps.sh /usr/local/lib/datahub/wait_for_deps.sh

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
