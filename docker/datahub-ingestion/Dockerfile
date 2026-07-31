# syntax=docker/dockerfile:1
# This image has two variants: full and slim.
# The full variant has a larger set of ingestion sources, but is a larger image.
ARG APP_ENV=full
ARG PYTHON_VERSION=3.10

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
    # Create and set proper permissions for datahub directories \
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME && \
    chmod g-s $HOME

ARG PYTHON_VERSION
RUN test -n "${PYTHON_VERSION}"  # PYTHON_VERSION must be set

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y \
    ca-certificates \
    git \
    wget \
    curl \
    zip \
    unzip \
    nano

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install Python via uv (python-build-standalone) instead of deadsnakes PPA.
# Avoids PPA availability issues and missing distutils on Noble (Ubuntu 24.04).
# UV_NO_CACHE=1 prevents root-owned cache files that cause permission errors
# when subsequent steps run as the datahub user.
ENV UV_PYTHON_INSTALL_DIR=/opt/python
ENV UV_NO_CACHE=1
RUN uv python install ${PYTHON_VERSION}

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

ARG UV_PROFILE=default
ARG DEFAULT_INDEX_URL=""
ARG EXTRA_INDEX_URLS=""
RUN --mount=type=bind,source=./docker/snippets/uv,target=/tmp/uv-config \
    mkdir -p $HOME/.config/uv && \
    if [ "${UV_PROFILE}" = "custom" ]; then \
        DEFAULT_INDEX_URL="${DEFAULT_INDEX_URL}" EXTRA_INDEX_URLS="${EXTRA_INDEX_URLS}" bash /tmp/uv-config/generate-config.sh > $HOME/.config/uv/uv.toml; \
    else \
        cp /tmp/uv-config/profiles/${UV_PROFILE}.toml $HOME/.config/uv/uv.toml; \
    fi


FROM python-base AS ingestion-base-slim

USER 0
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y \
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
    libodbc2
USER datahub

FROM ingestion-base-slim AS ingestion-base-full

USER 0
# We need to install build-essential in order to build some Python packages (e.g. python-ldap, kerberos)
# gcc and g++ are explicitly listed to ensure compilers are available on all architectures
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y \
    openjdk-25-jre-headless \
    build-essential \
    gcc \
    g++ \
    make

RUN --mount=type=bind,source=./docker/snippets/oracle_instantclient.sh,target=/oracle_instantclient.sh \
    /oracle_instantclient.sh

RUN --mount=type=bind,source=./docker/snippets/mssql_odbc.sh,target=/mssql_odbc.sh \
    /mssql_odbc.sh

USER datahub

# Locked variant uses the same base as slim (no JRE/Oracle needed)
FROM ingestion-base-slim AS ingestion-base-locked
# INLINE-END

# =============================================================================
# ADD-CODE STAGES - Separate stages for each variant to avoid caching issues
# with parameterized FROM statements in multi-platform builds
# =============================================================================

FROM ingestion-base-slim AS add-code-slim
COPY --chown=datahub ./metadata-ingestion /metadata-ingestion
ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"  # RELEASE_VERSION is a required build arg
RUN test -d /metadata-ingestion/src/datahub/metadata  # codegen must be run prior to building the image
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1

FROM ingestion-base-full AS add-code-full
COPY --chown=datahub ./metadata-ingestion /metadata-ingestion
ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN test -d /metadata-ingestion/src/datahub/metadata
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1

FROM ingestion-base-locked AS add-code-locked
COPY --chown=datahub ./metadata-ingestion /metadata-ingestion
ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN test -d /metadata-ingestion/src/datahub/metadata
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1

# =============================================================================
# FINAL STAGES - Ubuntu variants
# =============================================================================

FROM add-code-slim AS final-slim

ENV UV_BUILD_CONSTRAINT=/metadata-ingestion/build-constraints.txt

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    --mount=type=secret,id=netrc,target=/home/datahub/.netrc,required=false \
    UV_LINK_MODE=copy uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,s3-slim,gcs-slim,abs-slim,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]" && \
    datahub --version
RUN rm -rf $HOME/.config/uv

FROM add-code-full AS final-full

ENV UV_BUILD_CONSTRAINT=/metadata-ingestion/build-constraints.txt

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    --mount=type=secret,id=netrc,target=/home/datahub/.netrc,required=false \
    UV_LINK_MODE=copy uv pip install \
        -e "/metadata-ingestion/[all]" \
    && datahub --version
RUN rm -rf $HOME/.config/uv

FROM add-code-locked AS final-locked

ENV UV_BUILD_CONSTRAINT=/metadata-ingestion/build-constraints.txt

# Locked variant: minimal install with s3-slim, network will be blocked
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    --mount=type=secret,id=netrc,target=/home/datahub/.netrc,required=false \
    UV_LINK_MODE=copy uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,s3-slim,gcs-slim,abs-slim]" && \
    datahub --version
RUN rm -rf $HOME/.config/uv

# Block network access to PyPI in locked variant
ENV UV_DEFAULT_INDEX=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple


# =============================================================================
# DEFAULT EXPORT - Use APP_ENV to select variant (defaults to full)
# =============================================================================

FROM final-${APP_ENV} AS final

ENTRYPOINT [ "datahub" ]
