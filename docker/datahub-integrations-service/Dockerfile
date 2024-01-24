# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
ARG PIP_MIRROR_URL=null

# Base Image Common To All
FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

COPY ./datahub-integrations-service /datahub-integrations-service
COPY ./metadata-ingestion /metadata-ingestion

ARG RELEASE_VERSION
WORKDIR /datahub-integrations-service
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub_integrations/__init__.py && \
    cat src/datahub_integrations/__init__.py && \
    sed -i.bak "s/version = \"1!0.0.0.dev0\"/version = \"$RELEASE_VERSION\"/" pyproject.toml && \
    chown -R datahub /datahub-integrations-service /metadata-ingestion

USER datahub

# Build only for production/non-development
FROM base as build

ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi

RUN pip install --no-cache --user -r requirements.txt && \
    pip install --no-cache --user -e "."

# Production Image Built From Build Image
FROM base as prod-install

COPY --from=build /datahub-ingestion/.local /datahub-ingestion/.local

COPY ./docker/datahub-integrations-service/start.sh /start_datahub_integrations_service.sh
USER 0
RUN chmod a+x /start_datahub_integrations_service.sh
USER datahub

FROM prod-install as dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

# Bring in DataHub Actions
# COPY ./datahub-actions/datahub-actions/setup.py /datahub-actions/datahub-actions/
# COPY ./datahub-actions/datahub-actions/README.md /datahub-actions/datahub-actions/
# COPY ./datahub-actions/datahub-actions/src/datahub_actions/__init__.py /datahub-actions/datahub-actions/src/datahub_actions/__init__.py

ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-integrations-service/src --reload-dir /metadata-ingestion/src"

FROM ${APP_ENV}-install as final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9003/ping || exit 1
ENTRYPOINT []
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_integrations_service.sh
