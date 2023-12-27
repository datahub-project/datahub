# Defining environment
ARG APP_ENV=full
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head
ARG PIP_MIRROR_URL=null
ARG DEBIAN_REPO_URL=https://deb.debian.org/debian

FROM $BASE_IMAGE:$DOCKER_VERSION as base
USER 0

COPY ./metadata-ingestion /datahub-ingestion
COPY ./metadata-ingestion-modules/airflow-plugin /datahub-ingestion/airflow-plugin

ARG RELEASE_VERSION
WORKDIR /datahub-ingestion
RUN sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    sed -i.bak "s/__version__ = \"1!0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" airflow-plugin/src/datahub_airflow_plugin/__init__.py && \
    cat src/datahub/__init__.py && \
    chown -R datahub /datahub-ingestion

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"

FROM base as slim-install
ARG PIP_MIRROR_URL

RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi
RUN pip install --no-cache --user ".[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]"

FROM base as full-install-build
ARG PIP_MIRROR_URL
ARG DEBIAN_REPO_URL

USER 0
RUN if [ "${DEBIAN_REPO_URL}" != "http://deb.debian.org/debian" ] ; then sed -i "s#http.*://deb.debian.org/debian#${DEBIAN_REPO_URL}#g" /etc/apt/sources.list.d/debian.sources ; fi
RUN apt-get update && apt-get install -y -qq maven

USER datahub
COPY ./docker/datahub-ingestion/pyspark_jars.sh .

RUN if [ "${PIP_MIRROR_URL}" != "null" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi
RUN pip install --no-cache --user ".[base]" && \
    pip install --no-cache --user "./airflow-plugin[acryl-datahub-airflow-plugin]" && \
    pip install --no-cache --user ".[all]"
RUN ./pyspark_jars.sh

FROM base as full-install

COPY --from=full-install-build /datahub-ingestion/.local /datahub-ingestion/.local

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

USER datahub
ENV PATH="/datahub-ingestion/.local/bin:$PATH"
