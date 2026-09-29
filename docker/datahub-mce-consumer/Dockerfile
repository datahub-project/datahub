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
    && rm -f /tmp/setup_java_runtime.sh

COPY docker/snippets/wait_for_deps.sh /usr/local/lib/datahub/wait_for_deps.sh

FROM base AS prod-install
COPY metadata-jobs/mce-consumer-job/build/libs/mce-consumer-job.jar /datahub/datahub-mce-consumer/bin/
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-mce-consumer/resources/entity-registry.yml
COPY docker/datahub-mce-consumer/start.sh /datahub/datahub-mce-consumer/scripts/
COPY docker/monitoring/client-prometheus-config.yaml /datahub/datahub-mce-consumer/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-mce-consumer/scripts/start.sh

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-mce-consumer/resources/entity-registry.yml

FROM ${APP_ENV}-install AS final

RUN addgroup -S datahub && adduser -S datahub -G datahub && chmod g-s /home/datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""
ENV OTEL_EXPORTER_OTLP_MAX_PAYLOAD_SIZE=4194304 \
    OTEL_EXPORTER_OTLP_HTTP_HTTP2_MAX_FRAME_SIZE=16777215 \
    OTEL_EXPORTER_OTLP_HTTP_COMPRESSION=gzip \
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION=gzip

EXPOSE 9090
EXPOSE 4319

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:4319/actuator/health || curl --fail http://localhost:9090/actuator/health || exit 1

CMD /datahub/datahub-mce-consumer/scripts/start.sh
