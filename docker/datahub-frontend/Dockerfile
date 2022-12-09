# Defining environment
ARG APP_ENV=prod

FROM --platform=$BUILDPLATFORM alpine:3 AS base

RUN addgroup -S datahub && adduser -S datahub -G datahub

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add curl \
    && apk --no-cache add openjdk11-jre --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

FROM --platform=$BUILDPLATFORM node:16.13.0-alpine3.14 AS prod-build

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add perl openjdk8 openjdk11

# TODO: Does this do anything?
#ENV CI=true

COPY ./datahub-frontend/build/distributions/datahub-frontend.zip datahub-frontend.zip
RUN unzip datahub-frontend.zip

FROM base as prod-install
COPY --from=prod-build /datahub-frontend /datahub-frontend/
COPY ./docker/monitoring/client-prometheus-config.yaml /datahub-frontend/
RUN wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.18.0/opentelemetry-javaagent.jar -O opentelemetry-javaagent.jar \
    && wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar -O jmx_prometheus_javaagent.jar
RUN chown -R datahub:datahub /datahub-frontend && chmod 755 /datahub-frontend

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
