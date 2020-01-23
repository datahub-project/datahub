FROM openjdk:8
COPY --from=python:2.7 / /
COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-events:mxe-schemas:build \
    && cp -r metadata-ingestion/mce-cli . && cd metadata-ingestion/mce-cli \
    && pip install --user -r requirements.txt