# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

FROM golang:1-alpine3.21 AS binary

# Re-declaring arg from above to make it available in this stage (will inherit default value)
ARG ALPINE_REPO_URL

ENV DOCKERIZE_VERSION=v0.9.1
WORKDIR /go/src/github.com/jwilder

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM alpine:3.21 AS base

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
    && apk --no-cache add curl bash coreutils gcompat sqlite libc6-compat snappy \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && apk --no-cache add jattach --repository ${ALPINE_REPO_URL}/edge/community/ \
    && wget --no-verbose ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar \
    && wget --no-verbose ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-17-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
COPY --from=binary /go/bin/dockerize /usr/local/bin

ENV LD_LIBRARY_PATH="/lib:/lib64"

FROM base AS prod-install
COPY war.war /datahub/datahub-gms/bin/war.war
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml
COPY docker/datahub-gms/start.sh /datahub/datahub-gms/scripts/start.sh
COPY docker/monitoring/client-prometheus-config.yaml /datahub/datahub-gms/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-gms/scripts/start.sh

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final

RUN mkdir -p /etc/datahub/plugins/auth/resources

RUN addgroup -S datahub && adduser -S datahub -G datahub
RUN chown -R datahub:datahub /etc/datahub /datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""

EXPOSE 8080

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:8080/health || exit 1

CMD /datahub/datahub-gms/scripts/start.sh
