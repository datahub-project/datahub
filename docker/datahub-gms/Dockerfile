# Defining environment
ARG APP_ENV=prod

FROM openjdk:8-jre-alpine as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.20.v20190813/jetty-runner-9.4.20.v20190813.jar --output jetty-runner.jar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

FROM openjdk:8 as prod-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :gms:war:build
RUN cp /datahub-src/gms/war/build/libs/war.war /war.war

FROM base as prod-install
COPY --from=prod-build /war.war /datahub/datahub-gms/bin/war.war
COPY --from=prod-build /datahub-src/docker/datahub-gms/start.sh /datahub/datahub-gms/scripts/start.sh
RUN chmod +x /datahub/datahub-gms/scripts/start.sh

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup -S datahub && adduser -S datahub -G datahub
USER datahub

EXPOSE 8080

CMD /datahub/datahub-gms/scripts/start.sh