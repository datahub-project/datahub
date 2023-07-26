# Defining environment
ARG APP_ENV=prod
ARG DOCKER_VERSION=latest

FROM acryldata/datahub-ingestion-base:$DOCKER_VERSION as base

FROM acryldata/datahub-ingestion:$DOCKER_VERSION as prod-codegen

FROM base as prod-install

USER 0
WORKDIR /datahub-ingestion
COPY --from=prod-codegen /datahub-ingestion/ .

USER datahub
RUN pip install --user --no-deps ".[all]"

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
