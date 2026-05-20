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
    && mkdir -p /datahub/datahub-upgrade/lib \
    && INSTALL_JATTACH=0 OTEL_JMX_DIR=/datahub/datahub-upgrade/lib /tmp/setup_java_runtime.sh \
    && rm -f /tmp/setup_java_runtime.sh \
    && chmod -R a+r /datahub/datahub-upgrade/lib

COPY docker/snippets/wait_for_deps.sh /usr/local/lib/datahub/wait_for_deps.sh

FROM base AS prod-install
COPY datahub-upgrade/build/libs/datahub-upgrade.jar /datahub/datahub-upgrade/bin/
COPY docker/datahub-upgrade/start.sh /datahub/datahub-upgrade/scripts/start.sh
COPY docker/monitoring/client-prometheus-config.yaml /datahub/datahub-upgrade/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-upgrade/scripts/start.sh
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final

RUN addgroup -S datahub && adduser -S datahub -G datahub && chmod g-s /home/datahub
USER datahub

ENV OTEL_EXPORTER_OTLP_MAX_PAYLOAD_SIZE=4194304 \
    OTEL_EXPORTER_OTLP_HTTP_HTTP2_MAX_FRAME_SIZE=16777215 \
    OTEL_EXPORTER_OTLP_HTTP_COMPRESSION=gzip \
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION=gzip

ENTRYPOINT ["/datahub/datahub-upgrade/scripts/start.sh"]
