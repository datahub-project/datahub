# Defining environment
ARG APP_ENV=prod

FROM dockdevx/jwilderalpine:1.0 as base

# Workaround alpine issue with /lib64 not being in the ld library path
# https://gitlab.alpinelinux.org/alpine/aports/-/issues/10140
ENV LD_LIBRARY_PATH=/lib64

# Add glibc compat layer into alpine linux, needed by java-snappy if kafka topics are compressed with snappy
RUN apk add libc6-compat

FROM openjdk:8 as prod-build
COPY . datahub-src
RUN cd datahub-src && ./gradlew :datahub-gms-graphql-service:build
RUN cd datahub-src && cp datahub-gms-graphql-service/build/libs/datahub-gms-graphql-service.jar ../datahub-gms-graphql-service.jar

FROM base as prod-install
COPY --from=prod-build /datahub-gms-graphql-service.jar /datahub/datahub-gms-graphql-service/bin/
COPY --from=prod-build /datahub-src/docker/datahub-gms-graphql-service/start.sh /datahub/datahub-gms-graphql-service/scripts/
RUN chmod +x /datahub/datahub-gms-graphql-service/scripts/start.sh

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

EXPOSE 8091

CMD /datahub/datahub-gms-graphql-service/scripts/start.sh
