# Defining environment
ARG APP_ENV=prod

FROM openjdk:8-jre-alpine as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

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

EXPOSE 9090

CMD /datahub/datahub-mae-consumer/scripts/start.sh