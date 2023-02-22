# Defining environment
ARG APP_ENV=prod
ARG RELEASE_VERSION

FROM acryldata/datahub-ingestion:$RELEASE_VERSION as base

RUN pip uninstall pyspark
