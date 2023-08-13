# Defining environment
ARG APP_ENV=prod

FROM alpine:3 AS base

RUN addgroup -S datahub && adduser -S datahub -G datahub

# Upgrade Alpine and base packages
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk update

RUN apk --available upgrade \
    && apk add curl sqlite \
    && apk add openjdk11-jre \
    && apk add jattach

RUN rm -vrf /var/cache/apk/*

FROM base as prod-install

COPY docker/datahub-frontend.zip /
RUN unzip datahub-frontend.zip && rm datahub-frontend.zip
COPY ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/
RUN chown -R datahub:datahub /datahub-frontend && chmod 755 /datahub-frontend

ENV JMX_VERSION=0.18.0
RUN wget https://gh.api.99988866.xyz/https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar -O opentelemetry-javaagent.jar \
    && wget https://maven.aliyun.com/repository/central/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134
VOLUME [ "/datahub-frontend" ]

FROM ${APP_ENV}-install as final
USER datahub

ARG SERVER_PORT=9002
ENV SERVER_PORT=$SERVER_PORT
RUN echo $SERVER_PORT
EXPOSE $SERVER_PORT

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$SERVER_PORT/admin || exit 1
COPY ./docker/datahub-frontend/start.sh /
CMD ./start.sh
