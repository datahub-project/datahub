# Defining environment
ARG APP_ENV=prod

FROM openjdk:8-jre-alpine as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

# Workaround alpine issue with /lib64 not being in the ld library path
# https://gitlab.alpinelinux.org/alpine/aports/-/issues/10140
ENV LD_LIBRARY_PATH=/lib64

# Add glibc compat layer into alpine linux, needed by java-snappy if kafka topics are compressed with snappy
RUN apk add libc6-compat

FROM openjdk:8 as prod-build
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mae-consumer-job:build
RUN cd datahub-src && cp metadata-jobs/mae-consumer-job/build/libs/mae-consumer-job.jar ../mae-consumer-job.jar

FROM base as prod-install
COPY --from=prod-build /mae-consumer-job.jar /datahub/datahub-mae-consumer/bin/
COPY --from=prod-build /datahub-src/docker/datahub-mae-consumer/start.sh /datahub/datahub-mae-consumer/scripts/
RUN chmod +x /datahub/datahub-mae-consumer/scripts/start.sh

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

EXPOSE 9090

CMD /datahub/datahub-mae-consumer/scripts/start.sh