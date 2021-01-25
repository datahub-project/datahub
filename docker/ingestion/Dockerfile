# Defining environment
ARG APP_ENV=prod

FROM openjdk:8-jre-alpine as base

FROM openjdk:8 as prod-build
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-ingestion-examples:mce-cli:build

FROM base as prod-install
COPY --from=prod-build datahub-src/metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar /datahub/ingestion/bin/mce-cli.jar
COPY --from=prod-build datahub-src/metadata-ingestion-examples/mce-cli/example-bootstrap.json /datahub/ingestion/example-bootstrap.json

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final
CMD java -jar /datahub/ingestion/bin/mce-cli.jar -m produce /datahub/ingestion/example-bootstrap.json