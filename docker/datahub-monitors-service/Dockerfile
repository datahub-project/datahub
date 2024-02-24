# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
ARG PIP_MIRROR_URL=null

# Base Image Common To All
FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

FROM base as build

COPY ./datahub-monitors-service /datahub-monitors-service
COPY ./metadata-ingestion /metadata-ingestion
COPY ./docker/datahub-monitors-service/start.sh /start_datahub_monitors_service.sh
COPY ./docker/datahub-monitors-service/health_status.py /health_status.py
RUN chmod a+x /start_datahub_monitors_service.sh
RUN chmod a+x /health_status.py

WORKDIR /datahub-monitors-service

ARG RELEASE_VERSION
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub_monitors/__init__.py && \
    sed -i.bak "s/version = \"1!0.0.0.dev0\"/version = \"$RELEASE_VERSION\"/" pyproject.toml && \
    cat src/datahub_monitors/__init__.py && \
    chown -R datahub /metadata-ingestion /datahub-monitors-service

ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi

USER datahub
RUN pip install --no-cache --user -e .

WORKDIR /datahub-ingestion
RUN pip install --no-cache --user -e .

## Production Image Built From Build Image
FROM base as prod-install

COPY --from=build /datahub-monitors-service /datahub-monitors-service
COPY --from=build /datahub-ingestion/.local /datahub-ingestion/.local
COPY --from=build /start_datahub_monitors_service.sh /start_datahub_monitors_service.sh
COPY --from=build /health_status.py /health_status

USER datahub

FROM prod-install as dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

ENV PYTHONPATH="/metadata-ingestion/src:/datahub-monitors-service/src/datahub_monitors:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-monitors-service/src/datahub_monitors --reload-dir /metadata-ingestion/src"

FROM ${APP_ENV}-install as final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_monitors_service.sh
