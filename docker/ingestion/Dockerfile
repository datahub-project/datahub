FROM openjdk:8

COPY --from=python:3.6 / /
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-events:mxe-schemas:build \
    && cp -r metadata-ingestion/mce-cli . && cd metadata-ingestion/mce-cli \
    && pip install --user -r requirements.txt

CMD cd datahub-src/metadata-ingestion/mce-cli \
    && python mce_cli.py produce -d bootstrap_mce.dat -b $KAFKA_BOOTSTRAP_SERVER -s $KAFKA_SCHEMAREGISTRY_URL