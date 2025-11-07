# Defining environment
# APP_ENV can be prod or dev
ARG APP_ENV=prod
ARG PYTHON_VERSION=3.11
# BASE_IMAGE can be full or slim

ARG BASE_IMAGE=full
# INLINE-BEGIN @/docker/snippets/ingestion_base
# This is the "base" image workflow.
# While it has a bunch of intermediate stages, it "exports" a couple
# stages for consumption.
# - python-base: A basic stage, with basic deps, Python, and a venv.
# - ingestion-base-slim: Currently the same as base-empty.
# - ingestion-base-full: Adds a JRE and Oracle client.

FROM ubuntu:24.04 AS python-base

# TODO: This may not work on Ubuntu 24.04 due to the new deb822 package format.
ARG UBUNTU_REPO_URL=http://ports.ubuntu.com/ubuntu-ports
RUN if [ "${UBUNTU_REPO_URL}" != "http://ports.ubuntu.com/ubuntu-ports" ] ; then sed -i "s#http.*://ports.ubuntu.com/ubuntu-ports#${UBUNTU_REPO_URL}#g" /etc/apt/sources.list ; fi

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
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME && \
    chmod g-s $HOME

# Debug: Show actual datahub user info
RUN echo "=== DataHub User Debug Info ===" && \
    date && \
    getent passwd datahub && \
    getent group datahub && \
    id datahub

# Setup the PPA for alternative Python versions.
# TODO: Eventually we should switch to using uv's support for python-build-standalone.
RUN apt-get update && apt-get install -y \
    software-properties-common \
    lsb-release \
    gnupg \
    ca-certificates \
    && add-apt-repository --no-update ppa:deadsnakes/ppa \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION
RUN test -n "${PYTHON_VERSION}"  # PYTHON_VERSION must be set

RUN apt-get update && apt-get install -y \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-venv \
    python${PYTHON_VERSION}-dev \
    python${PYTHON_VERSION}-distutils \
    python-is-python3 \
    git \
    wget \
    curl \
    zip \
    unzip \
    nano \
    && rm -rf /var/lib/apt/lists/*

# Set the default python version.
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3 1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Configure pip/uv index URLs. Defaults to public PyPI but can be overridden at build time.
ARG PIP_MIRROR_URL=https://pypi.python.org/simple
ARG PIP_EXTRA_INDEX_URL=""
# Persist settings for subsequent build layers and for optional venv builds
ENV PIP_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
ENV UV_INDEX_URL=${PIP_MIRROR_URL}
# Also configure pip globally inside the image so tools without env still use the mirror
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi && \
    if [ -n "${PIP_EXTRA_INDEX_URL}" ] ; then uvx --no-cache pip config set global.extra-index-url ${PIP_EXTRA_INDEX_URL} ; fi

USER datahub
WORKDIR $HOME
RUN uv venv --python "$PYTHON_VERSION"
ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# We always want to use the system CA bundle.
# Requests comes with it's own CA bundle, which we need to override.
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
# uv uses a different mechanism. See https://github.com/astral-sh/uv/issues/1474.
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"


FROM python-base AS ingestion-base-slim

USER 0
RUN apt-get update && apt-get install -y \
    python3-ldap \
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
    ldap-utils \
    unixodbc \
    libodbc2 \
    && rm -rf /var/lib/apt/lists/*
USER datahub

FROM ingestion-base-slim AS ingestion-base-full

USER 0
# We need to install build-essential in order to build some Python packages (e.g. python-ldap)
RUN apt-get update && apt-get install --no-install-recommends -y -qq \
    default-jre-headless \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=bind,source=./docker/snippets/oracle_instantclient.sh,target=/oracle_instantclient.sh \
    /oracle_instantclient.sh

USER datahub
# INLINE-END


# =============================================================================
# PRE-BUILD BUNDLED INGESTION VENVS (optional; default off)
# =============================================================================
FROM ingestion-base-slim AS bundled-vEnvs

USER 0

# Control whether to build venvs in this image (default: is to build)
ARG BUILD_BUNDLED_VENVS=true

# Set up bundled venv configuration
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_CLI_VERSION
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}

# Create venv directory and ensure ownership
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy the self-contained venv build scripts into a stable location for reuse
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Make scripts executable
RUN chmod +x /usr/local/bin/build_bundled_venvs_unified.sh && \
    chmod +x /usr/local/bin/build_bundled_venvs_unified.py

USER datahub

# Optionally build bundled venvs (keeps scripts in image regardless)
RUN if [ "${BUILD_BUNDLED_VENVS}" = "true" ]; then \
      /usr/local/bin/build_bundled_venvs_unified.sh ; \
    else \
      echo "Skipping bundled venv build (BUILD_BUNDLED_VENVS=${BUILD_BUNDLED_VENVS})" ; \
    fi

# =============================================================================
# END DEFAULT VENVS SECTION
# =============================================================================


FROM ingestion-base-${BASE_IMAGE} AS build

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs

COPY --from=bundled-vEnvs $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH
COPY --from=bundled-vEnvs /usr/local/bin/build_bundled_venvs_unified.sh /usr/local/bin/build_bundled_venvs_unified.sh

USER 0
# Install Microsoft SQL Server ODBC driver
RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh
# Install jq, vim, and dockerize
RUN apt-get update \
    && apt-get install -y jq vim \
    && rm -rf /var/lib/apt/lists/*
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

# # Install JRE in the prod image
# RUN apt-get update -y && \
#     apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
#     rm -rf /var/lib/apt/lists/*

# # Set JAVA_HOME environment variable
# RUN ln -svT "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/default-jvm
# ENV JAVA_HOME=/usr/lib/jvm/default-jvm

# Install external deps in a cacheable layer
USER datahub
COPY ./datahub-executor/requirements.txt /datahub-executor/
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor \
  grep -v "\-e " /datahub-executor/requirements.txt | uv pip install -r /dev/stdin
USER 0

COPY --chown=datahub ./datahub-executor /datahub-executor
# copy to match requirements.txt entry: ../metadata-ingestion-modules/acryl-cloud (relative to /datahub-executor)
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service
RUN chmod a+x /start_datahub_executor.sh
RUN chmod a+x /health_status

WORKDIR /datahub-executor

ARG DOCKER_VERSION
ARG RELEASE_VERSION

RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /datahub-executor/ --version "$RELEASE_VERSION" --expected-update-count 2


# Embed build info into the image
RUN jq -n '{"docker_version": env.DOCKER_VERSION, "release_version": env.RELEASE_VERSION}' > /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

USER datahub
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor \
  uv pip install -r requirements.txt

RUN echo "Debug output:" && \
  uv pip list && \
  uv pip show acryl-datahub

## Production Image Built From Build Image
FROM build AS prod-install

USER datahub

FROM build AS dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

ENV PYTHONPATH="/metadata-ingestion-modules/acryl-cloud/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion-modules/acryl-cloud/src"
ENV DEBUGPY_ENABLED=true

FROM ${APP_ENV}-install AS final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD exec dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_executor.sh

