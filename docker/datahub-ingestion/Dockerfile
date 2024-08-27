# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head-full
ARG DEBIAN_REPO_URL=https://deb.debian.org/debian
ARG PIP_MIRROR_URL=https://pypi.python.org/simple

FROM $BASE_IMAGE:$DOCKER_VERSION AS base

# Optionally set corporate mirror for deb
USER 0
ARG DEBIAN_REPO_URL
RUN if [ "${DEBIAN_REPO_URL}" != "http://deb.debian.org/debian" ] ; then sed -i "s#http.*://deb.debian.org/debian#${DEBIAN_REPO_URL}#g" /etc/apt/sources.list.d/debian.sources ; fi
USER datahub

# Optionally set corporate mirror for pip
ARG PIP_MIRROR_URL
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}

COPY --chown=datahub ./metadata-ingestion /datahub-ingestion
COPY --chown=datahub ./metadata-ingestion-modules/airflow-plugin /datahub-ingestion/airflow-plugin

ARG RELEASE_VERSION
WORKDIR /datahub-ingestion
RUN sed -i.bak "s/__version__ = \"1\!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" src/datahub/__init__.py && \
    sed -i.bak "s/__version__ = \"1\!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" airflow-plugin/src/datahub_airflow_plugin/__init__.py && \
    cat src/datahub/__init__.py | grep __version__ && \
    cat airflow-plugin/src/datahub_airflow_plugin/__init__.py | grep __version__

FROM base AS slim-install

RUN uv pip install --no-cache -e ".[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]"

FROM base AS full-install-build

USER 0
RUN apt-get update && apt-get install -y -qq maven

USER datahub
COPY ./docker/datahub-ingestion/pyspark_jars.sh .

RUN uv pip install --no-cache -e ".[base,all]" "./airflow-plugin[plugin-v2]" && \
    datahub --version
RUN ./pyspark_jars.sh

FROM base AS full-install

COPY --from=full-install-build ${VIRTUAL_ENV} ${VIRTUAL_ENV}

FROM base AS dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final

USER datahub
