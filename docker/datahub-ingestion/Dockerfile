# Defining environment
ARG APP_ENV=prod

FROM acryldata/datahub-ingestion-base as base

FROM base as prod-codegen
COPY . /datahub-src
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
    pip freeze && \
    # This is required to fix security vulnerability in htrace-core4
    rm -f /usr/local/lib/python3.10/site-packages/pyspark/jars/htrace-core4-4.1.0-incubating.jar

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub

ENTRYPOINT [ "datahub" ]
