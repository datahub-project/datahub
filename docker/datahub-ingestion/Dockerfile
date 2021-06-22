# Defining environment
ARG APP_ENV=prod

FROM python:3.8-alpine as base

RUN apk add --no-cache \
        librdkafka-dev \
        openldap-dev \
        postgresql-dev \
        gcc \
        libc-dev \
    && python -m pip install --upgrade pip wheel setuptools

FROM openjdk:8 as prod-build
COPY . /datahub-src
# RUN cd /datahub-src && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
COPY --from=prod-build /datahub-src /datahub-src
# RUN cd /datahub-src/metadata-ingestion && \
#     pip install -e ".[base]" && \
#     ./scripts/codegen.sh

FROM base as prod-install
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
RUN cd /datahub-ingestion && \
    pip install ".[all]"

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub

ENTRYPOINT [ "datahub" ]