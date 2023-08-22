# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=latest

FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

COPY ./metadata-ingestion /datahub-ingestion

ARG RELEASE_VERSION
WORKDIR /datahub-ingestion
RUN sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    chown -R datahub /datahub-ingestion

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"

FROM base as slim-install
RUN pip install --no-cache --user ".[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]"

FROM base as full-install
RUN pip install --no-cache --user ".[all]"

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"
