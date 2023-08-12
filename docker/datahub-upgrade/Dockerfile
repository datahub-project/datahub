# Defining environment
ARG APP_ENV=prod

FROM golang:1-alpine3.17 AS binary

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk update
RUN apk add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM alpine:3 AS base

# Upgrade Alpine and base packages
ENV JMX_VERSION=0.18.0
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk update
RUN apk --available upgrade \
    && apk  add curl bash coreutils gcompat sqlite \
    && apk  add openjdk11-jre --repository=http://mirrors.ustc.edu.cn/alpine/edge/community \
    && curl -sS https://maven.aliyun.com/repository/central/org/eclipse/jetty/jetty-runner/9.4.46.v20220331/jetty-runner-9.4.46.v20220331.jar --output jetty-runner.jar \
    && curl -sS https://maven.aliyun.com/repository/central/org/eclipse/jetty/jetty-jmx/9.4.46.v20220331/jetty-jmx-9.4.46.v20220331.jar --output jetty-jmx.jar \
    && curl -sS https://maven.aliyun.com/repository/central/org/eclipse/jetty/jetty-util/9.4.46.v20220331/jetty-util-9.4.46.v20220331.jar --output jetty-util.jar \
    && wget --no-verbose https://gh.api.99988866.xyz/https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar \
    && wget --no-verbose https://maven.aliyun.com/repository/central/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-11-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
RUN rm -vrf /var/cache/apk/*
COPY --from=binary /go/bin/dockerize /usr/local/bin

FROM base as prod-install
COPY docker/datahub-upgrade.jar /datahub/datahub-upgrade/bin/
COPY metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENTRYPOINT ["java", "-jar", "/datahub/datahub-upgrade/bin/datahub-upgrade.jar"]
