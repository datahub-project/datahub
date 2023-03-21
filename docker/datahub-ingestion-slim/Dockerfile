# Defining environment
ARG APP_ENV=prod
ARG DOCKER_VERSION=latest

FROM acryldata/datahub-ingestion:$DOCKER_VERSION as base

USER 0
RUN pip uninstall -y pyspark
USER datahub
