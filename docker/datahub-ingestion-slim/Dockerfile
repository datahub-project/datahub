# Defining environment
ARG APP_ENV=prod
ARG DOCKER_VERSION=latest

FROM acryldata/datahub-ingestion-base:$DOCKER_VERSION as base

FROM base as prod-codegen
USER 0

COPY . /datahub-src

WORKDIR /datahub-src/metadata-ingestion
RUN pip install --user -e ".[base]" && \
    ./scripts/codegen.sh

FROM base as prod-install
USER 0

COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-codegen /root/.local/ /datahub-ingestion/.local/
# COPY --from=prod-codegen /root/.cache/pip /datahub-ingestion/.cache/pip

ARG RELEASE_VERSION
WORKDIR /datahub-ingestion
RUN sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    chown -R datahub /datahub-ingestion

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
