# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

FROM golang:1-alpine3.18 AS binary

# Re-declaring arg from above to make it available in this stage (will inherit default value)
ARG ALPINE_REPO_URL

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM alpine:3.18 AS base

# Re-declaring args from above to make them available in this stage (will inherit default values)
ARG ALPINE_REPO_URL
ARG GITHUB_REPO_URL
ARG MAVEN_CENTRAL_REPO_URL

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# Upgrade Alpine and base packages
ENV JMX_VERSION=0.18.0
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add curl bash coreutils gcompat sqlite libc6-compat java-snappy \
    && apk --no-cache add openjdk11-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && curl -sS ${MAVEN_CENTRAL_REPO_URL}/org/eclipse/jetty/jetty-runner/9.4.46.v20220331/jetty-runner-9.4.46.v20220331.jar --output jetty-runner.jar \
    && curl -sS ${MAVEN_CENTRAL_REPO_URL}/org/eclipse/jetty/jetty-jmx/9.4.46.v20220331/jetty-jmx-9.4.46.v20220331.jar --output jetty-jmx.jar \
    && curl -sS ${MAVEN_CENTRAL_REPO_URL}/org/eclipse/jetty/jetty-util/9.4.46.v20220331/jetty-util-9.4.46.v20220331.jar --output jetty-util.jar \
    && wget --no-verbose ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar \
    && wget --no-verbose ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-11-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
COPY --from=binary /go/bin/dockerize /usr/local/bin

ENV LD_LIBRARY_PATH="/lib:/lib64"

FROM base as prod-install
COPY datahub-upgrade.jar /datahub/datahub-upgrade/bin/
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENTRYPOINT ["java", "-jar", "/datahub/datahub-upgrade/bin/datahub-upgrade.jar"]
