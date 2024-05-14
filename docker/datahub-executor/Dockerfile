# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
ARG PIP_MIRROR_URL=null
ARG VIRTUAL_ENV=/datahub-ingestion/.venv

# Base Image Common To All
FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

FROM base as build

COPY ./datahub-executor /datahub-executor
COPY ./metadata-ingestion /metadata-ingestion
COPY ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY ./docker/datahub-executor/health_status.py /health_status
COPY ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh
RUN chmod a+x /health_status

WORKDIR /datahub-executor

ARG RELEASE_VERSION
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed 's/+/-/'|sed '1 s/-/+/')\"/" src/datahub_executor/__init__.py && \
    sed -i.bak "s/version = \"1!0.0.0.dev0\"/version = \"$(echo $RELEASE_VERSION|sed 's/+/-/'|sed '1 s/-/+/')\"/" pyproject.toml && \
    cat src/datahub_executor/__init__.py && \
    chown -R datahub /metadata-ingestion /datahub-executor

ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi

USER datahub
RUN uv pip install --no-cache -e .

WORKDIR /datahub-ingestion
RUN uv pip install --no-cache -e .

## Production Image Built From Build Image
FROM build as prod-install

USER datahub

FROM prod-install as dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

ENV PYTHONPATH="/metadata-ingestion/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion/src"

FROM ${APP_ENV}-install as final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_executor.sh
