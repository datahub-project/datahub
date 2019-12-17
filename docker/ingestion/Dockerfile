FROM openjdk:8
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM python:2.7-slim
WORKDIR /ingestion

COPY metadata-ingestion/mce-cli /ingestion
RUN pip install -r requirements.txt
COPY metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc /ingestion
RUN rm -rf datahub-src