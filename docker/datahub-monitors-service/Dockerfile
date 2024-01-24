# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
ARG PIP_MIRROR_URL=null

# Base Image Common To All
FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

COPY ./datahub-monitors-service /datahub-monitors-service
COPY ./metadata-ingestion /metadata-ingestion

ARG RELEASE_VERSION
WORKDIR /datahub-monitors-service
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub_monitors/__init__.py && \
    cat src/datahub_monitors/__init__.py && \
    sed -i.bak "s/version = \"1!0.0.0.dev0\"/version = \"$RELEASE_VERSION\"/" pyproject.toml && \
    chown -R datahub /datahub-monitors-service /metadata-ingestion

USER datahub

ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false

# Build only for production/non-development
FROM base as build

ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi

# Why venv? https://github.com/python-poetry/poetry/issues/1214
RUN pip install --no-cache --user poetry==1.4.2 && \
    python3 -m venv /metadata-ingestion/.local && \
    . /metadata-ingestion/.local/bin/activate && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi && \
    poetry export --without-hashes && \
    deactivate && \
    pip install --no-cache --user -e "."

# Production Image Built From Build Image
FROM base as prod-install

COPY --from=build /datahub-ingestion/.local /datahub-ingestion/.local

COPY ./docker/datahub-monitors-service/start.sh /start_datahub_monitors_service.sh
USER 0
RUN chmod a+x /start_datahub_monitors_service.sh
USER datahub

FROM prod-install as dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-monitors-service/src/datahub_monitors"

FROM ${APP_ENV}-install as final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_monitors_service.sh
