# Defining environment
ARG APP_ENV=prod
ARG DOCKER_VERSION=latest

FROM acryldata/datahub-ingestion-slim:$DOCKER_VERSION as base

FROM base as prod-install
WORKDIR /datahub-ingestion

RUN pip install --user ".[all]"

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final
