# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

FROM alpine:3.22 AS base

# Re-declaring args from above to make them available in this stage (will inherit default values)
ARG ALPINE_REPO_URL
ARG GITHUB_REPO_URL
ARG MAVEN_CENTRAL_REPO_URL

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

ENV JMX_VERSION=0.20.0

# Upgrade Alpine and base packages
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add 'c-ares>1.34.5'  --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add 'curl>8.17.0' --repository=${ALPINE_REPO_URL}/edge/main \
    && apk --no-cache add bash coreutils gcompat sqlite libc6-compat \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && apk --no-cache add snappy=~1.2 --repository=${ALPINE_REPO_URL}/edge/main \
    && mkdir -p /datahub/datahub-upgrade/lib \
    && wget --no-verbose -P /datahub/datahub-upgrade/lib ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.15.0/opentelemetry-javaagent.jar \
    && wget --no-verbose -P /datahub/datahub-upgrade/lib ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar \
    && chmod -R a+r /datahub/datahub-upgrade/lib \
    && cp /usr/lib/jvm/java-17-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin

ENV KUBECTL_VERSION=v1.34.3
RUN set -x \
  && APK_ARCH="$(cat /etc/apk/arch)" \
  && case "$APK_ARCH" in \
     x86_64) \
       KUBECTL_ARCH=amd64 \
       ;; \
     aarch64) \
       KUBECTL_ARCH=arm64 \
       ;; \
     esac \
    && curl -L "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${KUBECTL_ARCH}/kubectl" | install -o root -g root -m 0755 /dev/stdin /usr/local/bin/kubectl

ENV LD_LIBRARY_PATH="/lib:/lib64"

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
