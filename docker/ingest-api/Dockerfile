FROM python:3.8-slim as base
RUN apt-get update && apt-get install -y jq

# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim as base
FROM openjdk:8 as prod-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

FROM base as final
ENV PROMETHEUS_MULTIPROC_DIR=/gunicorn/
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
RUN cd /datahub-ingestion && \
    pip install .
COPY ./ingest-api/ /ingest-api/
WORKDIR /ingest-api/ingest_api
RUN pip install -r ../requirements.txt && pip install ../ && mkdir /gunicorn/
CMD ["gunicorn", "-c", "config.py", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8001"]

