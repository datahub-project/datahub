# Defining environment
ARG APP_ENV=prod

FROM adoptopenjdk/openjdk8:alpine-jre as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar wget bash coreutils \
    && wget --no-verbose https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.4.1/opentelemetry-javaagent-all.jar \
    && wget --no-verbose https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar -O jmx_prometheus_javaagent.jar \
    && curl -sSL https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

FROM adoptopenjdk/openjdk8:alpine-slim as prod-build
RUN apk --no-cache add openjdk8-jre perl
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mae-consumer-job:build -x test
RUN cd datahub-src && cp metadata-jobs/mae-consumer-job/build/libs/mae-consumer-job.jar ../mae-consumer-job.jar

FROM base as prod-install
COPY --from=prod-build /mae-consumer-job.jar /datahub/datahub-mae-consumer/bin/
COPY --from=prod-build /datahub-src/metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-mae-consumer/resources/entity-registry.yml
COPY --from=prod-build /datahub-src/docker/datahub-mae-consumer/start.sh /datahub/datahub-mae-consumer/scripts/
COPY --from=prod-build /datahub-src/docker/monitoring/client-prometheus-config.yaml /datahub/datahub-mae-consumer/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-mae-consumer/scripts/start.sh

# Setup TLS JKS
COPY --from=prod-build /usr/lib/jvm/java-1.8-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""

EXPOSE 9090

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9091/actuator/health || exit 1

CMD /datahub/datahub-mae-consumer/scripts/start.sh