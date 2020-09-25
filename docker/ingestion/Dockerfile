FROM openjdk:8 as builder
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-ingestion-examples:mce-cli:build

FROM openjdk:8-jre-alpine
COPY --from=builder datahub-src/metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar ./
COPY --from=builder datahub-src/metadata-ingestion-examples/mce-cli/example-bootstrap.json ./
CMD java -jar mce-cli.jar -m produce example-bootstrap.json