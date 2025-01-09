# Defining environment
ARG APP_ENV=prod

FROM alpine:3.21 AS base

# Configurable repositories
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

RUN addgroup -S datahub && adduser -S datahub -G datahub

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# Upgrade Alpine and base packages
# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
ENV JMX_VERSION=0.18.0
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add curl sqlite libc6-compat snappy \
    && apk --no-cache add openjdk17-jre-headless --repository=${ALPINE_REPO_URL}/edge/community \
    && apk --no-cache add jattach --repository ${ALPINE_REPO_URL}/edge/community/ \
    && wget ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar -O opentelemetry-javaagent.jar \
    && wget ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar

ENV LD_LIBRARY_PATH="/lib:/lib64"

FROM base AS unpack

COPY ./datahub-frontend.zip /
RUN unzip datahub-frontend.zip -d /tmp/out \
   && mv /tmp/out/main /datahub-frontend
COPY ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/
RUN chown -R datahub:datahub /datahub-frontend && chmod 755 /datahub-frontend

FROM base AS prod-install

COPY --from=unpack /datahub-frontend/ /datahub-frontend/

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134
VOLUME [ "/datahub-frontend" ]

FROM ${APP_ENV}-install AS final
COPY --chown=datahub:datahub --chmod=755 ./docker/datahub-frontend/start.sh /
USER datahub

ARG SERVER_PORT=9002
ENV SERVER_PORT=$SERVER_PORT
RUN echo $SERVER_PORT
EXPOSE $SERVER_PORT

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$SERVER_PORT/admin || exit 1
CMD ./start.sh
