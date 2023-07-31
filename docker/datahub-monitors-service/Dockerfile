# Defining environment
ARG APP_ENV=prod

FROM golang:1-alpine3.17 AS binary
ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder
RUN apk --no-cache --update add openssl git tar curl
WORKDIR /go/src/github.com/jwilder/dockerize
RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION


FROM python:3.10 as base
COPY --from=binary /go/bin/dockerize /usr/local/bin
RUN pip install poetry==1.4.2

FROM eclipse-temurin:11 as prod-build
COPY . /datahub-src
WORKDIR /datahub-src

# NOTE: Copied from ../datahub-ingestion/Dockerfile.
RUN (for attempt in 1 2 3 4 5; do ./gradlew --version && break ; echo "Failed to download gradle wrapper (attempt $attempt)" && sleep $((2<<$attempt)) ; done ) && \
    ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen

COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

FROM base as prod-install

ARG RELEASE_VERSION

ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-src/metadata-ingestion
COPY --from=prod-codegen /datahub-src/datahub-monitors-service /datahub-src/datahub-monitors-service
COPY --from=prod-codegen /root/.cache/pip /root/.cache/pip
RUN cd /datahub-src/datahub-monitors-service && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub_monitors/__init__.py && \
    cat src/datahub_monitors/__init__.py && \
    sed -i.bak "s/version = \"0.0.0.dev0\"/version = \"$RELEASE_VERSION\"/" pyproject.toml && \
    poetry config virtualenvs.create false && \
    poetry install && \
    poetry export --without-hashes

COPY ./docker/datahub-monitors-service/start.sh /start_datahub_monitors_service.sh
RUN chmod a+x /start_datahub_monitors_service.sh

FROM base as dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false
COPY ./datahub-monitors-service/pyproject.toml ./datahub-monitors-service/poetry.lock /datahub-src/datahub-monitors-service/
# We only add these so that we can run `poetry install` without errors.
COPY ./datahub-monitors-service/src/datahub_monitors/__init__.py /datahub-src/datahub-monitors-service/src/datahub_monitors/__init__.py
COPY ./metadata-ingestion/setup.py /datahub-src/metadata-ingestion/
COPY ./metadata-ingestion/README.md /datahub-src/metadata-ingestion/
COPY ./metadata-ingestion/src/datahub/__init__.py /datahub-src/metadata-ingestion/src/datahub/__init__.py
RUN cd /datahub-src/datahub-monitors-service && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi && \
    pip freeze

ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-src/datahub-monitors-service/src/datahub_monitors"

FROM ${APP_ENV}-install as final

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub
WORKDIR /datahub-src/datahub-monitors-service
HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_monitors_service.sh
