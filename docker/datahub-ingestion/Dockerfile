# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head
ARG DEBIAN_REPO_URL=https://deb.debian.org/debian
ARG PIP_MIRROR_URL=https://pypi.python.org/simple
ARG DATAHUB_PIP_PACKAGE_NAME=acryl-datahub
ARG AIRFLOW_PLUGIN_PIP_PACKAGE_NAME=acryl-datahub-airflow-plugin

FROM $BASE_IMAGE:$DOCKER_VERSION as base

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
ENV DATAHUB_INIT_FILE="src/datahub/__init__.py"
ENV AIRFLOW_PLUGIN_INIT_FILE="airflow-plugin/src/datahub_airflow_plugin/__init__.py"
ARG DATAHUB_PIP_PACKAGE_NAME
ARG AIRFLOW_PLUGIN_PIP_PACKAGE_NAME
RUN sed -i.bak "s/__version__ = \"1\!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" ${DATAHUB_INIT_FILE} && \
    sed -i.bak "s/__version__ = \"1\!0.0.0.dev0\"/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" ${AIRFLOW_PLUGIN_INIT_FILE} && \
    sed -i.bak "s/\(^__package_name__\s*=\s*\).*/\1\"${DATAHUB_PIP_PACKAGE_NAME}\"/" "${DATAHUB_INIT_FILE}" && \
    sed -i.bak "s/\(^__package_name__\s*=\s*\).*/\1\"${AIRFLOW_PLUGIN_PIP_PACKAGE_NAME}\"/" "${AIRFLOW_PLUGIN_INIT_FILE}" && \
    sed -i.bak "s/\(^DATAHUB_PACKAGE_NAME\s*=\s*\).*/\1\"${DATAHUB_PIP_PACKAGE_NAME}\"/" "${AIRFLOW_PLUGIN_INIT_FILE}" && \
    cat ${DATAHUB_INIT_FILE} && \
    cat ${AIRFLOW_PLUGIN_INIT_FILE}

FROM base as slim-install
ARG DATAHUB_PIP_PACKAGE_NAME

RUN uv pip install --no-cache "${DATAHUB_PIP_PACKAGE_NAME}[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary] @ ."

FROM base as full-install-build
ARG DATAHUB_PIP_PACKAGE_NAME
ARG AIRFLOW_PLUGIN_PIP_PACKAGE_NAME

USER 0
RUN apt-get update && apt-get install -y -qq maven

USER datahub
COPY ./docker/datahub-ingestion/pyspark_jars.sh .

RUN uv pip install --prerelease=allow --no-cache "${DATAHUB_PIP_PACKAGE_NAME}[base,all] @ ." "${AIRFLOW_PLUGIN_PIP_PACKAGE_NAME}[plugin-v2] @ ./airflow-plugin" && \
    datahub --version
RUN ./pyspark_jars.sh

FROM base as full-install

COPY --from=full-install-build ${VIRTUAL_ENV} ${VIRTUAL_ENV}

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
