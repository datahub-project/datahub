# Defining environment
ARG APP_ENV=prod

FROM alpine:3.20 AS base

# Configurable repositories
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
ARG GITHUB_REPO_URL=https://github.com
ARG MAVEN_CENTRAL_REPO_URL=https://repo1.maven.org/maven2

# Add user and set up repository mirrors
RUN addgroup -S datahub && adduser -S datahub -G datahub \
    && if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ]; then \
    sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories; \
    fi

# Upgrade packages and install dependencies
ENV JMX_VERSION=0.18.0 \
    LD_LIBRARY_PATH="/lib:/lib64"

RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add curl sqlite libc6-compat snappy \
    && apk --no-cache add openjdk17-jre-headless jattach --repository=${ALPINE_REPO_URL}/edge/community \
    && wget ${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.24.0/opentelemetry-javaagent.jar -O opentelemetry-javaagent.jar \
    && wget ${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar -O jmx_prometheus_javaagent.jar

FROM base as prod-install

COPY ./datahub-frontend.zip /
RUN unzip datahub-frontend.zip -d /datahub-frontend \
    && mv /datahub-frontend/main/* /datahub-frontend \
    && rmdir /datahub-frontend/main \
    && rm datahub-frontend.zip \
    && chown -R datahub:datahub /datahub-frontend \
    && chmod 755 /datahub-frontend

COPY ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
VOLUME [ "/datahub-frontend" ]

FROM ${APP_ENV}-install as final

COPY ./docker/datahub-frontend/start.sh /
RUN chown datahub:datahub /start.sh && chmod 755 /start.sh
USER datahub

ARG SERVER_PORT=9002
ENV SERVER_PORT=$SERVER_PORT

EXPOSE $SERVER_PORT

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:$SERVER_PORT/admin || exit 1
CMD ./start.sh
