# This image has two variants: full and slim.
# The full variant has a larger set of ingestion sources, but is a larger image.
ARG APP_ENV=full

FROM ubuntu:24.04 AS base-slim

# INLINE-BEGIN @/docker/snippets/ubuntu_mirror_setup
# TODO: This may not work on Ubuntu 24.04 due to the new deb822 package format.
ARG UBUNTU_REPO_URL=http://ports.ubuntu.com/ubuntu-ports
RUN if [ "${UBUNTU_REPO_URL}" != "http://ports.ubuntu.com/ubuntu-ports" ] ; then sed -i "s#http.*://ports.ubuntu.com/ubuntu-ports#${UBUNTU_REPO_URL}#g" /etc/apt/sources.list ; fi
# INLINE-END

ENV PYTHON_VERSION=3.10
# INLINE-BEGIN @/docker/snippets/ubuntu_python_base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG PIP_MIRROR_URL=https://pypi.python.org/simple
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}

RUN apt-get update && apt-get install --no-install-recommends -y -qq \
    libldap2-dev \
    libsasl2-dev \
    libsasl2-modules \
    libaio-dev \
    libaio1t64 \
    libsasl2-modules-gssapi-mit \
    krb5-user \
    krb5-config \
    libkrb5-dev \
    librdkafka-dev \
    git \
    wget \
    curl \
    zip \
    unzip \
    ldap-utils \
    unixodbc \
    libodbc2 \
    && rm -rf /var/lib/apt/lists/*

ENV HOME=/home/datahub
RUN existing_group=$(getent group 1000 | cut -d: -f1) && \
    if [ -n "$existing_group" ] && [ "$existing_group" != "datahub" ]; then \
        echo "Renaming existing group $existing_group to datahub"; \
        groupmod -n datahub "$existing_group"; \
    elif [ -z "$existing_group" ]; then \
        echo "Creating new group datahub with GID 1000"; \
        addgroup --gid 1000 datahub; \
    fi && \
    existing_user=$(id -nu 1000 2>/dev/null || echo "") && \
    if [ -n "$existing_user" ] && [ "$existing_user" != "datahub" ]; then \
        echo "Renaming existing user $existing_user to datahub"; \
        usermod -l datahub -d $HOME "$existing_user"; \
        usermod -g datahub datahub; \
    elif [ -z "$existing_user" ]; then \
        echo "Creating new user datahub with UID 1000"; \
        adduser --disabled-password --uid 1000 --gid 1000 --home $HOME datahub; \
    fi && \
    # Create and set proper permissions for datahub directories
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME

USER datahub
WORKDIR $HOME
RUN uv venv --python "$PYTHON_VERSION"
ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
# Requests comes with it's own CA bundle, but we want to use always use the system CA bundle.
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
# INLINE-END

FROM base-slim AS full-deps-prebuild

USER 0
RUN apt-get update && apt-get install --no-install-recommends -y -qq \
    build-essential \
    maven \
    && rm -rf /var/lib/apt/lists/*
USER datahub

RUN uv pip install python-ldap==3.4.4
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000 \
    --mount=type=bind,source=./docker/datahub-ingestion/pyspark_jars.sh,target=/pyspark_jars.sh \
    uv pip install python-ldap==3.4.4 pyspark~=3.5.0 && \
    /pyspark_jars.sh

FROM base-slim AS base-full

USER 0
# INLINE-BEGIN @/docker/snippets/ingestion_full_deps
RUN apt-get update && apt-get install --no-install-recommends -y -qq \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN if [ $(arch) = "x86_64" ]; then \
    mkdir /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/2115000/instantclient-basic-linux.x64-21.15.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-21.15.0.0.0dbru.zip && \
    rm instantclient-basic-linux.x64-21.15.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_21_15 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    else \
    mkdir /opt/oracle && \
    cd /opt/oracle && \
    wget --no-verbose -c https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip && \
    rm instantclient-basic-linux.arm64-19.23.0.0.0dbru.zip && \
    sh -c "echo /opt/oracle/instantclient_19_23 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig; \
    fi;
# INLINE-END
USER datahub

RUN --mount=from=full-deps-prebuild,source=$HOME/.venv,target=/venv-full \
    rm -r .venv && \
    cp -r /venv-full .venv

FROM base-${APP_ENV} AS add-code

COPY --chown=datahub ./metadata-ingestion /metadata-ingestion
COPY --chown=datahub ./metadata-ingestion-modules/airflow-plugin /airflow-plugin

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN test -d /metadata-ingestion/src/datahub/metadata  # codegen must be run prior to building the image
RUN sed -i.bak "s/__version__ = .*$/__version__ = \"$RELEASE_VERSION\"/" /metadata-ingestion/src/datahub/_version.py && \
    sed -i.bak "s/__version__ = .*$/__version__ = \"$RELEASE_VERSION\"/" /airflow-plugin/src/datahub_airflow_plugin/_version.py && \
    cat /metadata-ingestion/src/datahub/_version.py | grep __version__ && \
    cat /airflow-plugin/src/datahub_airflow_plugin/_version.py | grep __version__

FROM add-code AS install-slim

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000 \
    UV_LINK_MODE=copy uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,hive,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]" && \
    datahub --version

FROM add-code AS install-full

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000 \
    UV_LINK_MODE=copy uv pip install \
        -e "/metadata-ingestion/[base,all]" \
        -e "/airflow-plugin/[plugin-v2]" && \
    datahub --version

FROM install-${APP_ENV} AS final

ENTRYPOINT [ "datahub" ]
