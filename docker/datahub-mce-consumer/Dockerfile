# Defining environment
ARG APP_ENV=prod

FROM alpine:3.14 AS base

ENV DOCKERIZE_VERSION v0.6.1

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && if [ $(arch) = "aarch64" ]; then \
       DOCKERIZE_ARCH='aarch64';\
    elif [ $(arch) = "x86_64" ]; then \
      DOCKERIZE_ARCH='amd64'; \
    else \
      echo >&2 "Unsupported architecture $(arch)" ; exit 1; \
    fi \
    && apk --no-cache add tar curl bash \
    && apk --no-cache add openjdk11-jre --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community \
    && wget --no-verbose https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.18.0/opentelemetry-javaagent.jar \
    && wget --no-verbose https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-11-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks \
    && curl -sSL https://github.com/treff7es/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-${DOCKERIZE_ARCH}-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

FROM --platform=$BUILDPLATFORM alpine:3.14.2 AS prod-build

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add openjdk8 openjdk11 perl

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mce-consumer-job:build
RUN cd datahub-src && cp metadata-jobs/mce-consumer-job/build/libs/mce-consumer-job.jar ../mce-consumer-job.jar

FROM base as prod-install
COPY --from=prod-build /mce-consumer-job.jar /datahub/datahub-mce-consumer/bin/
COPY --from=prod-build /datahub-src/docker/datahub-mce-consumer/start.sh /datahub/datahub-mce-consumer/scripts/
COPY --from=prod-build /datahub-src/docker/monitoring/client-prometheus-config.yaml /datahub/datahub-mce-consumer/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-mce-consumer/scripts/start.sh

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""

EXPOSE 9090

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9090/actuator/health || exit 1

CMD /datahub/datahub-mce-consumer/scripts/start.sh