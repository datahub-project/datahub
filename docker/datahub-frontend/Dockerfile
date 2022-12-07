# Defining environment
ARG APP_ENV=prod

FROM alpine:3.14 AS base

RUN addgroup -S datahub && adduser -S datahub -G datahub

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add curl \
    && apk --no-cache add openjdk11-jre --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

FROM --platform=$BUILDPLATFORM node:16.13.0-alpine3.14 AS prod-build

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add perl openjdk8 openjdk11

ARG USE_SYSTEM_NODE="true"
ENV CI=true
ENV GRADLE_OPTS="-Xms256m -Xmx512m"

# copy only what's realy needed for the build
# to get the benefits of caching as much as possible
COPY ./datahub-frontend ./datahub-src/datahub-frontend
COPY ./entity-registry ./datahub-src/entity-registry
COPY ./buildSrc ./datahub-src/buildSrc
COPY ./datahub-web-react ./datahub-src/datahub-web-react
COPY ./li-utils ./datahub-src/li-utils
COPY ./metadata-models ./datahub-src/metadata-models
COPY ./metadata-models-validator ./datahub-src/metadata-models-validator
COPY ./metadata-utils ./datahub-src/metadata-utils
COPY ./metadata-service ./datahub-src/metadata-service
COPY ./metadata-io ./datahub-src/metadata-io
COPY ./datahub-graphql-core ./datahub-src/datahub-graphql-core
COPY ./metadata-auth ./datahub-src/metadata-auth
COPY ./metadata-integration ./datahub-src/metadata-integration
COPY ./gradle ./datahub-src/gradle
COPY repositories.gradle gradle.properties gradlew settings.gradle build.gradle ./datahub-src/

RUN cd datahub-src \
    && ./gradlew :datahub-web-react:build -x test -x yarnTest -x yarnLint \
    && ./gradlew :datahub-frontend:dist -PuseSystemNode=${USE_SYSTEM_NODE} -x test -x yarnTest -x yarnLint \
    && cp datahub-frontend/build/distributions/datahub-frontend.zip ../datahub-frontend.zip \
    && cd .. && rm -rf datahub-src && unzip datahub-frontend.zip

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
