# Defining environment
ARG APP_ENV=prod

FROM openjdk:8-jre-alpine as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

# Workaround alpine issue with /lib64 not being in the ld library path
# https://gitlab.alpinelinux.org/alpine/aports/-/issues/10140
ENV LD_LIBRARY_PATH=/lib64

FROM openjdk:8 as prod-build
COPY . datahub-src
RUN cd datahub-src && ./gradlew :datahub-upgrade:build
RUN cd datahub-src && cp datahub-upgrade/build/libs/datahub-upgrade.jar ../datahub-upgrade.jar

FROM base as prod-install
COPY --from=prod-build /datahub-upgrade.jar /datahub/datahub-upgrade/bin/

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

ENTRYPOINT ["java", "-jar", "/datahub/datahub-upgrade/bin/datahub-upgrade.jar"]
