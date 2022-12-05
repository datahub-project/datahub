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
    && apk --no-cache add tar curl bash coreutils gcompat \
    && apk --no-cache add openjdk11-jre --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community \
    && curl -sS https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.46.v20220331/jetty-runner-9.4.46.v20220331.jar --output jetty-runner.jar \
    && curl -sS https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-jmx/9.4.46.v20220331/jetty-jmx-9.4.46.v20220331.jar --output jetty-jmx.jar \
    && curl -sS https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.46.v20220331/jetty-util-9.4.46.v20220331.jar --output jetty-util.jar \
    && wget --no-verbose https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.18.0/opentelemetry-javaagent.jar \
    && wget --no-verbose https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar -O jmx_prometheus_javaagent.jar \
    && cp /usr/lib/jvm/java-11-openjdk/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks \
    && curl -sS -L https://github.com/treff7es/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-${DOCKERIZE_ARCH}-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

FROM --platform=$BUILDPLATFORM alpine:3.14 AS prod-build

# Upgrade Alpine and base packages
RUN apk --no-cache --update-cache --available upgrade \
    && apk --no-cache add openjdk8 openjdk11 perl

COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-service:war:build -x test
RUN cp /datahub-src/metadata-service/war/build/libs/war.war /war.war

FROM base as prod-install
COPY --from=prod-build /war.war /datahub/datahub-gms/bin/war.war
COPY --from=prod-build /datahub-src/metadata-models/src/main/resources/entity-registry.yml /datahub/datahub-gms/resources/entity-registry.yml
COPY --from=prod-build /datahub-src/docker/datahub-gms/start.sh /datahub/datahub-gms/scripts/start.sh
COPY --from=prod-build /datahub-src/docker/datahub-gms/jetty.xml /datahub/datahub-gms/scripts/jetty.xml
COPY --from=prod-build /datahub-src/docker/monitoring/client-prometheus-config.yaml /datahub/datahub-gms/scripts/prometheus-config.yaml
RUN chmod +x /datahub/datahub-gms/scripts/start.sh

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN mkdir -p /etc/datahub/plugins/auth/resources

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENV JMX_OPTS=""
ENV JAVA_OPTS=""

EXPOSE 8080

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:8080/health || exit 1

CMD /datahub/datahub-gms/scripts/start.sh
