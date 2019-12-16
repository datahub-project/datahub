FROM openjdk:8
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM python:2.7-slim
WORKDIR /ingestion
COPY docker/ingestion/mce-cli/requirements.txt ./
RUN pip install -r requirements.txt
COPY docker/ingestion/mce-cli /ingestion
COPY metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc /ingestion
RUN rm -rf datahub-src
CMD [ "python", "mce_cli.py",  "produce" , "-d" , "bootstrap_mce.dat" ]