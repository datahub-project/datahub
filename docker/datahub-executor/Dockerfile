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
    build-essential \
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
# PRE-BUILD BUNDLED INGESTION VENVS - FULL VARIANT
# =============================================================================
FROM ingestion-base-slim AS bundled-venvs-full
USER 0

# Set up bundled venv configuration for FULL variant (with PySpark)
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_VENV_SLIM_MODE="false"
ARG BUNDLED_CLI_VERSION
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_VENV_SLIM_MODE=${BUNDLED_VENV_SLIM_MODE}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}

# Create venv directory and ensure ownership
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy the self-contained venv build scripts into a stable location for reuse
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Make scripts executable
RUN chmod +x /usr/local/bin/build_bundled_venvs_unified.sh && \
    chmod +x /usr/local/bin/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (standard s3 with PySpark)
WORKDIR /tmp
RUN /usr/local/bin/build_bundled_venvs_unified.sh

USER datahub

# =============================================================================
# PRE-BUILD BUNDLED INGESTION VENVS - SLIM VARIANT
# =============================================================================
FROM ingestion-base-slim AS bundled-venvs-slim
USER 0

# Set up bundled venv configuration for SLIM variant (without PySpark)
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_VENV_SLIM_MODE="true"
ARG BUNDLED_CLI_VERSION
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_VENV_SLIM_MODE=${BUNDLED_VENV_SLIM_MODE}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}

# Create venv directory and ensure ownership
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy the self-contained venv build scripts into a stable location for reuse
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Make scripts executable
RUN chmod +x /usr/local/bin/build_bundled_venvs_unified.sh && \
    chmod +x /usr/local/bin/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (s3-slim without PySpark)
WORKDIR /tmp
RUN /usr/local/bin/build_bundled_venvs_unified.sh

USER datahub

# =============================================================================
# END BUNDLED VENVS SECTION
# =============================================================================
# Note: locked variant reuses bundled-venvs-slim (identical configuration)

# =============================================================================
# BUILD STAGE - FULL VARIANT
# =============================================================================
FROM ingestion-base-full AS build-full

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs

COPY --from=bundled-venvs-full $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH
COPY --from=bundled-venvs-full /usr/local/bin/build_bundled_venvs_unified.py /usr/local/bin/build_bundled_venvs_unified.py
COPY --from=bundled-venvs-full /usr/local/bin/build_bundled_venvs_unified.sh /usr/local/bin/build_bundled_venvs_unified.sh

USER 0
# Install Microsoft SQL Server ODBC driver
RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh
# Install jq, vim, and dockerize
RUN apt-get update \
    && apt-get install -y jq vim \
    && rm -rf /var/lib/apt/lists/*
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

# Install external deps in a cacheable layer
USER datahub
COPY ./datahub-executor/requirements.txt /datahub-executor/
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor \
  grep -v "^\-e " /datahub-executor/requirements.txt | uv pip install -r /dev/stdin
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

# =============================================================================
# BUILD STAGE - SLIM/LOCKED BASE (shared by both slim and locked variants)
# =============================================================================
# This base stage contains all the common setup for slim and locked variants.
# Uses s3-slim instead of s3 to avoid PySpark and its bundled JARs.
FROM ingestion-base-slim AS build-slim-base

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs

# Copy bundled venvs from slim stage (used by both slim and locked)
COPY --from=bundled-venvs-slim $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH
COPY --from=bundled-venvs-slim /usr/local/bin/build_bundled_venvs_unified.py /usr/local/bin/
COPY --from=bundled-venvs-slim /usr/local/bin/build_bundled_venvs_unified.sh /usr/local/bin/

USER 0
# Install Microsoft SQL Server ODBC driver
RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh
# Install jq, vim, and dockerize
RUN apt-get update \
    && apt-get install -y jq vim \
    && rm -rf /var/lib/apt/lists/*
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

# Copy full source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service
RUN chmod a+x /start_datahub_executor.sh
RUN chmod a+x /health_status

WORKDIR /datahub-executor

# Modify pyproject.toml: use s3-slim instead of s3, remove unity-catalog (both require PySpark)
RUN echo "=== Before sed ===" && grep "acryl-datahub\[" /datahub-executor/pyproject.toml && \
  sed -i 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,snowflake,bigquery,redshift,s3-slim]/g' /datahub-executor/pyproject.toml && \
  echo "=== After sed ===" && grep "acryl-datahub\[" /datahub-executor/pyproject.toml

ARG DOCKER_VERSION
ARG RELEASE_VERSION

RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /datahub-executor/ --version "$RELEASE_VERSION" --expected-update-count 2

# Embed build info into the image
RUN jq -n '{"docker_version": env.DOCKER_VERSION, "release_version": env.RELEASE_VERSION}' > /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

# Install all dependencies including editable packages
# Using direct install instead of compile step for reliability
# Use separate cache ID for slim variants to avoid cross-contamination
USER datahub
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-slim \
  uv pip install --reinstall -e /metadata-ingestion-modules/acryl-cloud -e .

# Reinstall packages with extras to ensure entry points are created
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-slim \
  uv pip install "watchdog[watchmedo]==3.0.0" "uvicorn[standard]>=0.23.2"

# Verify critical binaries and no PySpark
# Show package list first for debugging, then verify requirements
RUN echo "=== Installed packages ===" && \
  uv pip list && \
  echo "=== Checking binaries ===" && \
  ls -la $VIRTUAL_ENV/bin/ | grep -E "(watchmedo|uvicorn)" || true && \
  echo "=== Verification ===" && \
  python -c "import pyspark" 2>/dev/null && echo "FAIL: PySpark found" && exit 1 || echo "OK: No PySpark" && \
  which watchmedo || (echo "FAIL: watchmedo not found" && exit 1) && \
  which uvicorn || (echo "FAIL: uvicorn not found" && exit 1) && \
  python -c "import fastapi" || (echo "FAIL: fastapi not importable" && exit 1) && \
  echo "=== All checks passed ==="

# =============================================================================
# BUILD STAGE - SLIM VARIANT
# =============================================================================
FROM build-slim-base AS build-slim
USER datahub

# =============================================================================
# BUILD STAGE - LOCKED VARIANT
# =============================================================================
# Locked variant extends slim-base and blocks network access at runtime
FROM build-slim-base AS build-locked
# Block network access to PyPI - locked variant only uses bundled venvs
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple
USER datahub

# =============================================================================
# BUILD STAGE - SLIM UNBUNDLED VARIANT (no pre-built bundled venvs)
# =============================================================================
FROM ingestion-base-slim AS build-slim-unbundled

USER 0
# Install Microsoft SQL Server ODBC driver
RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh
# Install jq, vim, and dockerize
RUN apt-get update \
    && apt-get install -y jq vim \
    && rm -rf /var/lib/apt/lists/*
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

# Copy full source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service
RUN chmod a+x /start_datahub_executor.sh
RUN chmod a+x /health_status

WORKDIR /datahub-executor

# Modify pyproject.toml: use s3-slim instead of s3, remove unity-catalog (both require PySpark)
RUN echo "=== Before sed ===" && grep "acryl-datahub\[" /datahub-executor/pyproject.toml && \
  sed -i 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,snowflake,bigquery,redshift,s3-slim]/g' /datahub-executor/pyproject.toml && \
  echo "=== After sed ===" && grep "acryl-datahub\[" /datahub-executor/pyproject.toml

ARG DOCKER_VERSION
ARG RELEASE_VERSION

RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /datahub-executor/ --version "$RELEASE_VERSION" --expected-update-count 2

# Embed build info into the image
RUN jq -n '{"docker_version": env.DOCKER_VERSION, "release_version": env.RELEASE_VERSION}' > /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

# Install all dependencies including editable packages - use --no-cache to avoid stale resolutions
USER datahub
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-slim-unbundled \
  uv pip install --reinstall -e /metadata-ingestion-modules/acryl-cloud -e .

# Reinstall packages with extras to ensure entry points are created
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-slim-unbundled \
  uv pip install "watchdog[watchmedo]==3.0.0" "uvicorn[standard]>=0.23.2"

# Verify critical binaries (no PySpark in slim variants)
RUN echo "=== Installed packages ===" && uv pip list && \
  echo "=== Verification ===" && \
  python -c "import pyspark" 2>/dev/null && echo "FAIL: PySpark found" && exit 1 || echo "OK: No PySpark" && \
  which watchmedo || (echo "FAIL: watchmedo not found" && exit 1) && \
  which uvicorn || (echo "FAIL: uvicorn not found" && exit 1) && \
  python -c "import fastapi" || (echo "FAIL: fastapi not importable" && exit 1) && \
  echo "=== All checks passed ==="

# =============================================================================
# BUILD STAGE - FULL UNBUNDLED VARIANT (no pre-built bundled venvs)
# =============================================================================
FROM ingestion-base-full AS build-full-unbundled

USER 0
# Install Microsoft SQL Server ODBC driver
RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh
# Install jq, vim, and dockerize
RUN apt-get update \
    && apt-get install -y jq vim \
    && rm -rf /var/lib/apt/lists/*
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

# Install external deps in a cacheable layer first
USER datahub
COPY ./datahub-executor/requirements.txt /datahub-executor/
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor \
  grep -v "^\-e " /datahub-executor/requirements.txt | uv pip install -r /dev/stdin
USER 0

COPY --chown=datahub ./datahub-executor /datahub-executor
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

# Install all packages including editable
USER datahub
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor \
  uv pip install -r requirements.txt

# Verify critical binaries
RUN echo "=== Installed packages ===" && uv pip list && \
  echo "=== Verification ===" && \
  which watchmedo || (echo "FAIL: watchmedo not found" && exit 1) && \
  which uvicorn || (echo "FAIL: uvicorn not found" && exit 1) && \
  python -c "import fastapi" || (echo "FAIL: fastapi not importable" && exit 1) && \
  echo "=== All checks passed ==="

# =============================================================================
# PROD INSTALL STAGES - ONE PER BASE_IMAGE VARIANT
# =============================================================================
FROM build-full AS prod-install-full
USER datahub

FROM build-slim AS prod-install-slim
USER datahub

FROM build-locked AS prod-install-locked
USER datahub

FROM build-slim-unbundled AS prod-install-slim-unbundled
USER datahub

FROM build-full-unbundled AS prod-install-full-unbundled
USER datahub

# =============================================================================
# DEV INSTALL STAGES - ONE PER BASE_IMAGE VARIANT
# =============================================================================
FROM build-full AS dev-install-full
# Development stage: enables hot reload and remote debugging
# See this excellent thread https://github.com/docker/cli/issues/1134
RUN uv pip install debugpy
ENV PYTHONPATH="/metadata-ingestion-modules/acryl-cloud/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion-modules/acryl-cloud/src"
ENV DEBUGPY_ENABLED=true

FROM build-slim AS dev-install-slim
RUN uv pip install debugpy
ENV PYTHONPATH="/metadata-ingestion-modules/acryl-cloud/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion-modules/acryl-cloud/src"
ENV DEBUGPY_ENABLED=true

FROM build-slim-unbundled AS dev-install-slim-unbundled
RUN uv pip install debugpy
ENV PYTHONPATH="/metadata-ingestion-modules/acryl-cloud/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion-modules/acryl-cloud/src"
ENV DEBUGPY_ENABLED=true

FROM build-full-unbundled AS dev-install-full-unbundled
RUN uv pip install debugpy
ENV PYTHONPATH="/metadata-ingestion-modules/acryl-cloud/src:/datahub-executor/src/datahub_executor:${PYTHONPATH}"
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-executor/src/datahub_executor --reload-dir /metadata-ingestion-modules/acryl-cloud/src"
ENV DEBUGPY_ENABLED=true

# =============================================================================
# ALPINE-BASED VARIANTS
# =============================================================================

# Alpine runtime base - minimal packages for running the application
FROM alpine:3.22 AS alpine-base

ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

ENV HOME=/home/datahub
RUN addgroup -g 1000 -S datahub && \
    adduser -u 1000 -S datahub -G datahub -h $HOME && \
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME

# Install runtime dependencies only (no build tools, no vim due to CVE-2025-66476)
RUN apk --no-cache add \
    ca-certificates \
    curl \
    bash \
    jq \
    libffi \
    libstdc++ \
    && apk --no-cache add librdkafka --repository=${ALPINE_REPO_URL}/edge/community

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY --from=powerman/dockerize:0.19 /usr/local/bin/dockerize /usr/local/bin

ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# We always want to use the system CA bundle
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"

# Alpine build stage - extends runtime base with dev packages and build tools
FROM alpine-base AS alpine-build

ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
# Alpine uses Python 3.11 for better musl wheel availability
ARG PYTHON_VERSION=3.11
ARG PIP_MIRROR_URL=https://pypi.python.org/simple
ARG PIP_EXTRA_INDEX_URL=""

# Add dev packages and build tools
RUN apk --no-cache add \
    python3 \
    python3-dev \
    py3-pip \
    git \
    wget \
    unzip \
    build-base \
    libffi-dev \
    openssl-dev \
    linux-headers \
    musl-dev \
    gcc \
    g++ \
    make \
    clang \
    compiler-rt \
    && apk --no-cache add librdkafka-dev --repository=${ALPINE_REPO_URL}/edge/community

RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi && \
    if [ -n "${PIP_EXTRA_INDEX_URL}" ] ; then uvx --no-cache pip config set global.extra-index-url ${PIP_EXTRA_INDEX_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}

# clang required for python-build-standalone compatibility
ENV CC=clang
ENV CXX=clang++

USER datahub
WORKDIR $HOME
RUN uv venv --python "$PYTHON_VERSION"

# =============================================================================
# VENV BUILDER - ALPINE VARIANT
# Note: Alpine excludes snowflake/bigquery/redshift due to spacy/blis build issues on musl/ARM64
# =============================================================================
FROM alpine-build AS venv-builder-alpine

USER 0

# Copy source code needed for building
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service

WORKDIR /datahub-executor

# Modify pyproject.toml for Alpine compatibility:
# 1. Use minimal extras (avoids classification_lib/spacy/blis)
# 2. Remove prophet (requires CmdStan which doesn't build on musl/ARM64)
RUN echo "=== Modifying pyproject.toml for Alpine ===" && \
  sed -i 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,datahub-rest,datahub-kafka,s3-slim]/g' /datahub-executor/pyproject.toml && \
  sed -i '/"prophet==/d' /datahub-executor/pyproject.toml && \
  echo "=== After modifications ===" && grep -E "acryl-datahub\[|prophet" /datahub-executor/pyproject.toml || echo "prophet removed"

ARG DOCKER_VERSION
ARG RELEASE_VERSION

RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python3 /version_updater.py --directory /datahub-executor/ --version "$RELEASE_VERSION" --expected-update-count 2

# Embed build info into the image
RUN jq -n '{"docker_version": env.DOCKER_VERSION, "release_version": env.RELEASE_VERSION}' > /build_info.json

# Install all dependencies (no cache mount for Alpine to avoid wheel compatibility issues)
USER datahub
RUN uv pip install --reinstall -e /metadata-ingestion-modules/acryl-cloud -e .

# Reinstall packages with extras to ensure entry points are created
# Also install sqlglot explicitly (needed by executor but not in minimal extras)
RUN uv pip install "watchdog[watchmedo]==3.0.0" "uvicorn[standard]>=0.23.2" "sqlglot>=23.0.0"

# Verify critical binaries, no PySpark, and sqlglot available
RUN echo "=== Installed packages ===" && \
  uv pip list && \
  echo "=== Verification ===" && \
  python3 -c "import pyspark" 2>/dev/null && echo "FAIL: PySpark found" && exit 1 || echo "OK: No PySpark" && \
  which watchmedo || (echo "FAIL: watchmedo not found" && exit 1) && \
  which uvicorn || (echo "FAIL: uvicorn not found" && exit 1) && \
  python3 -c "import fastapi" || (echo "FAIL: fastapi not importable" && exit 1) && \
  python3 -c "import sqlglot" || (echo "FAIL: sqlglot not importable" && exit 1) && \
  echo "=== All checks passed ==="


# =============================================================================
# VENV BUILDER - LOCKED ALPINE VARIANT (network blocked in final stage)
# =============================================================================
FROM venv-builder-alpine AS venv-builder-locked-alpine
USER datahub


# =============================================================================
# FINAL STAGE - ALPINE (runtime only, no build tools)
# =============================================================================
FROM alpine-base AS prod-install-alpine

USER root

# Copy the venv and Python from the builder stage
COPY --from=venv-builder-alpine --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv
COPY --from=venv-builder-alpine --chown=datahub:datahub /home/datahub/.local/share/uv/python /home/datahub/.local/share/uv/python

# Copy source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service

# Copy startup files and configs
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh && chmod a+x /health_status

# Copy build info from builder
COPY --from=venv-builder-alpine /build_info.json /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

USER datahub
WORKDIR /datahub-executor


# =============================================================================
# FINAL STAGE - LOCKED ALPINE (runtime only, network blocked)
# =============================================================================
FROM alpine-base AS prod-install-locked-alpine

USER root

# Copy the venv and Python from the builder stage
COPY --from=venv-builder-locked-alpine --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv
COPY --from=venv-builder-locked-alpine --chown=datahub:datahub /home/datahub/.local/share/uv/python /home/datahub/.local/share/uv/python

# Copy source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service

# Copy startup files and configs
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh && chmod a+x /health_status

# Copy build info from builder
COPY --from=venv-builder-locked-alpine /build_info.json /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

# Block network access to PyPI - locked variant only uses bundled venvs
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

USER datahub
WORKDIR /datahub-executor


# =============================================================================
# WOLFI-BASED VARIANTS (glibc-based, supports full dependency set)
# =============================================================================

# Wolfi runtime base - minimal packages for running the application
# Uses glibc instead of musl, enabling full dependency support
# For now we must pin wolfi-base to latest until we get access to Chainguard
# Using wolfi-base:latest is acceptable for now but no SLAs for remediation.

FROM cgr.dev/chainguard/wolfi-base:latest AS wolfi-base

ENV HOME=/home/datahub
USER root
RUN addgroup -g 1000 datahub && \
    adduser -u 1000 -G datahub -h $HOME -D datahub && \
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME

# Install runtime dependencies
RUN apk update && apk add --no-cache \
    ca-certificates \
    curl \
    bash \
    jq \
    libffi \
    libstdc++ \
    dockerize \
    uv

ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# We always want to use the system CA bundle
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"


# Wolfi build stage - extends runtime base with dev packages and build tools
FROM wolfi-base AS wolfi-build

ARG PYTHON_VERSION=3.11
ARG PIP_MIRROR_URL=https://pypi.python.org/simple
ARG PIP_EXTRA_INDEX_URL=""

USER root
# Add Python 3.11 and dev packages
# Note: Wolfi bundles C++ in gcc, no separate g++ package - use libstdc++-dev for C++ headers
RUN apk update && apk add --no-cache \
    python-3.11 \
    python-3.11-dev \
    py3.11-pip \
    git \
    wget \
    unzip \
    build-base \
    libffi-dev \
    openssl-dev \
    linux-headers \
    libstdc++-dev

# Configure pip/uv index URLs
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uv pip config set global.index-url ${PIP_MIRROR_URL} || true ; fi && \
    if [ -n "${PIP_EXTRA_INDEX_URL}" ] ; then uv pip config set global.extra-index-url ${PIP_EXTRA_INDEX_URL} || true ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}

USER datahub
WORKDIR $HOME
RUN uv venv --python "3.11"


# =============================================================================
# VENV BUILDER - WOLFI VARIANT
# Uses full extras since glibc supports all dependencies including prophet
# =============================================================================
FROM wolfi-build AS venv-builder-wolfi

USER root

# Copy source code needed for building
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service
COPY --chown=datahub ./docker/snippets/ingestion/constraints.txt /tmp/constraints.txt

WORKDIR /datahub-executor

# Modify pyproject.toml for Wolfi: use s3-slim to avoid PySpark, but keep full platform support
# Unlike Alpine, we can keep snowflake/bigquery/redshift since glibc supports spacy/blis
RUN echo "=== Modifying pyproject.toml for Wolfi ===" && \
  sed -i 's/\[base,snowflake,bigquery,redshift,s3,unity-catalog\]/[base,snowflake,bigquery,redshift,s3-slim]/g' /datahub-executor/pyproject.toml && \
  echo "=== After modifications ===" && grep -E "acryl-datahub\[|prophet" /datahub-executor/pyproject.toml

ARG DOCKER_VERSION
ARG RELEASE_VERSION
ARG GITHUB_RUN_ID=""
ARG BUNDLED_VENV_PLUGINS=""

RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python3 /version_updater.py --directory /datahub-executor/ --version "$RELEASE_VERSION" --expected-update-count 2

# Embed build info into the image (including provenance and bundled plugins)
RUN jq -n \
    --arg docker_version "$DOCKER_VERSION" \
    --arg release_version "$RELEASE_VERSION" \
    --arg github_run_id "$GITHUB_RUN_ID" \
    --arg bundled_plugins "$BUNDLED_VENV_PLUGINS" \
    '{docker_version: $docker_version, release_version: $release_version, github_run_id: $github_run_id, bundled_plugins: (if $bundled_plugins != "" then ($bundled_plugins | split(",")) else [] end)}' \
    > /build_info.json

# Install all dependencies with constraints to ensure version compatibility
# (e.g., cmdstanpy==1.2.5 is required for prophet==1.1.5)
USER datahub
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-wolfi \
  uv pip install --reinstall --constraint /tmp/constraints.txt -e /metadata-ingestion-modules/acryl-cloud -e .

# Reinstall packages with extras to ensure entry points are created
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-executor-wolfi \
  uv pip install "watchdog[watchmedo]==3.0.0" "uvicorn[standard]>=0.23.2"

# Verify critical binaries and no PySpark
RUN echo "=== Installed packages ===" && \
  uv pip list && \
  echo "=== Verification ===" && \
  python3 -c "import pyspark" 2>/dev/null && echo "FAIL: PySpark found" && exit 1 || echo "OK: No PySpark" && \
  which watchmedo || (echo "FAIL: watchmedo not found" && exit 1) && \
  which uvicorn || (echo "FAIL: uvicorn not found" && exit 1) && \
  python3 -c "import fastapi" || (echo "FAIL: fastapi not importable" && exit 1) && \
  echo "=== All checks passed ==="


# =============================================================================
# VENV BUILDER - LOCKED WOLFI VARIANT (network blocked in final stage)
# =============================================================================
FROM venv-builder-wolfi AS venv-builder-locked-wolfi
USER datahub


# =============================================================================
# PRE-BUILD BUNDLED INGESTION VENVS - WOLFI VARIANT
# =============================================================================
FROM wolfi-build AS bundled-venvs-wolfi
USER root

# Set up bundled venv configuration for WOLFI variant (without PySpark, like slim)
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_VENV_SLIM_MODE="true"
ARG BUNDLED_CLI_VERSION
ARG GITHUB_RUN_ID=""
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_VENV_SLIM_MODE=${BUNDLED_VENV_SLIM_MODE}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}
ENV GITHUB_RUN_ID=${GITHUB_RUN_ID}

# Create venv directory and ensure ownership
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Create /usr/bin/python symlink for Wolfi (uv venv expects it)
# Wolfi only has python3.11, not a generic 'python' symlink like Ubuntu
RUN ln -sf /usr/bin/python3.11 /usr/bin/python

# Copy the self-contained venv build scripts into a stable location for reuse
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /usr/local/bin/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Make scripts executable
RUN chmod +x /usr/local/bin/build_bundled_venvs_unified.sh && \
    chmod +x /usr/local/bin/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (slim mode, no PySpark)
WORKDIR /tmp
RUN /usr/local/bin/build_bundled_venvs_unified.sh

# Create metadata file for bundled venvs (provenance tracking)
RUN jq -n \
    --arg plugins "$BUNDLED_VENV_PLUGINS" \
    --arg cli_version "$BUNDLED_CLI_VERSION" \
    --arg slim_mode "$BUNDLED_VENV_SLIM_MODE" \
    --arg github_run_id "$GITHUB_RUN_ID" \
    '{bundled_plugins: ($plugins | split(",")), cli_version: $cli_version, slim_mode: ($slim_mode == "true"), github_run_id: $github_run_id}' \
    > ${DATAHUB_BUNDLED_VENV_PATH}/metadata.json

USER datahub


# =============================================================================
# FINAL STAGE - WOLFI (runtime only, no build tools)
# =============================================================================
FROM wolfi-base AS prod-install-wolfi

USER root

# Install Python runtime (no dev packages)
RUN apk update && apk add --no-cache python-3.11-base

# Create /usr/bin/python symlink for Wolfi (bundled venvs expect it)
RUN ln -sf /usr/bin/python3.11 /usr/bin/python

# Copy the venv and Python from the builder stage
COPY --from=venv-builder-wolfi --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv

# Copy bundled venvs and build scripts from bundled-venvs-wolfi stage
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-wolfi $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH
COPY --from=bundled-venvs-wolfi /usr/local/bin/build_bundled_venvs_unified.py /usr/local/bin/
COPY --from=bundled-venvs-wolfi /usr/local/bin/build_bundled_venvs_unified.sh /usr/local/bin/

# Copy source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service

# Copy startup files and configs
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh && chmod a+x /health_status

# Copy build info from builder
COPY --from=venv-builder-wolfi /build_info.json /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

USER datahub
WORKDIR /datahub-executor


# =============================================================================
# FINAL STAGE - LOCKED WOLFI (runtime only, network blocked)
# =============================================================================
FROM wolfi-base AS prod-install-locked-wolfi

USER root

# Install Python runtime (no dev packages)
RUN apk update && apk add --no-cache python-3.11-base

# Create /usr/bin/python symlink for Wolfi (bundled venvs expect it)
RUN ln -sf /usr/bin/python3.11 /usr/bin/python

# Copy the venv and Python from the builder stage
COPY --from=venv-builder-locked-wolfi --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv

# Copy bundled venvs and build scripts from bundled-venvs-wolfi stage
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-wolfi $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH
COPY --from=bundled-venvs-wolfi /usr/local/bin/build_bundled_venvs_unified.py /usr/local/bin/
COPY --from=bundled-venvs-wolfi /usr/local/bin/build_bundled_venvs_unified.sh /usr/local/bin/

# Copy source code
COPY --chown=datahub ./datahub-executor /datahub-executor
COPY --chown=datahub ./metadata-ingestion-modules/acryl-cloud /metadata-ingestion-modules/acryl-cloud
COPY --chown=datahub ./datahub-integrations-service /datahub-integrations-service

# Copy startup files and configs
COPY --chown=datahub ./docker/datahub-executor/start.sh /start_datahub_executor.sh
COPY --chown=datahub ./docker/datahub-executor/health_status.py /health_status
COPY --chown=datahub ./docker/datahub-executor/ingestion-pipeline-config.yaml /etc/datahub-executor/ingestion-pipeline-config.yaml
RUN chmod a+x /start_datahub_executor.sh && chmod a+x /health_status

# Copy build info from builder
COPY --from=venv-builder-locked-wolfi /build_info.json /build_info.json

# Temporarily symlink acryl cloud package for backward compatibility
RUN ln -sv /metadata-ingestion-modules/acryl-cloud /acryl-cloud

# Block network access to PyPI - locked variant only uses bundled venvs
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

USER datahub
WORKDIR /datahub-executor


# =============================================================================
# FINAL STAGE SELECTOR - Uses APP_ENV and BASE_IMAGE to select the right variant
# =============================================================================
ARG APP_ENV=prod
ARG BASE_IMAGE=full

FROM ${APP_ENV}-install-${BASE_IMAGE} AS final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9004/health || exit 1
ENTRYPOINT []
CMD exec dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_executor.sh

