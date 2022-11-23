# Defining environment
ARG APP_ENV=prod

FROM acryldata/datahub-ingestion-base as base

FROM openjdk:11 as prod-build
COPY . /datahub-src
RUN cd /datahub-src && ./gradlew :wrapper && ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

FROM base as prod-install
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-codegen /root/.cache/pip /root/.cache/pip
ARG RELEASE_VERSION
RUN cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    pip install ".[all]" && \
    pip freeze

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub

ENTRYPOINT [ "datahub" ]
