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
    # Create and set proper permissions for datahub directories
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME && \
    chmod g-s $HOME

# Setup the PPA for alternative Python versions.
# TODO: Eventually we should switch to using uv's support for python-build-standalone.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y \
    software-properties-common \
    lsb-release \
    gnupg \
    ca-certificates \
    && add-apt-repository --no-update ppa:deadsnakes/ppa

ARG PYTHON_VERSION
RUN test -n "${PYTHON_VERSION}"  # PYTHON_VERSION must be set

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y \
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
    nano

# Set the default python version.
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3 1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG PIP_MIRROR_URL=https://pypi.python.org/simple
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}

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
    default-jre-headless \
    build-essential \
    gcc \
    g++ \
    make

RUN --mount=type=bind,source=./docker/snippets/oracle_instantclient.sh,target=/oracle_instantclient.sh \
    /oracle_instantclient.sh

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

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    UV_LINK_MODE=copy uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,snowflake,bigquery,redshift,mysql,postgres,s3-slim,gcs-slim,abs-slim,clickhouse,glue,dbt,looker,lookml,tableau,powerbi,superset,datahub-business-glossary]" && \
    datahub --version

FROM add-code-full AS final-full

RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    UV_LINK_MODE=copy uv pip install \
        -e "/metadata-ingestion/[all]" \
    && datahub --version

FROM add-code-locked AS final-locked

# Locked variant: minimal install with s3-slim, network will be blocked
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,sharing=private \
    UV_LINK_MODE=copy uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,s3-slim,gcs-slim,abs-slim]" && \
    datahub --version

# Block network access to PyPI in locked variant
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

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

# Runtime packages only (no -dev packages, no build tools)
RUN apk --no-cache --update-cache --available upgrade && \
    apk --no-cache add \
    bash \
    curl \
    ca-certificates \
    libldap \
    libsasl \
    cyrus-sasl-gssapiv2 \
    krb5-libs \
    unixodbc \
    libffi \
    && apk --no-cache add librdkafka --repository=${ALPINE_REPO_URL}/edge/community

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"


# Alpine build stage - extends runtime base with dev packages and build tools
FROM alpine-base AS alpine-build

ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
# Alpine uses Python 3.11 for better musl wheel availability (pydantic-core, etc.)
ARG PYTHON_VERSION=3.11
ARG PIP_MIRROR_URL=https://pypi.python.org/simple

# Add dev packages and build tools (runtime deps already in base)
RUN apk --no-cache add \
    git \
    openldap-dev \
    cyrus-sasl-dev \
    krb5-dev \
    unixodbc-dev \
    libffi-dev \
    build-base \
    clang \
    compiler-rt \
    && apk --no-cache add librdkafka-dev --repository=${ALPINE_REPO_URL}/edge/community

RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi
ENV UV_INDEX_URL=${PIP_MIRROR_URL}

# clang required for python-build-standalone compatibility
ENV CC=clang
ENV CXX=clang++

USER datahub
WORKDIR $HOME
RUN uv venv --python "$PYTHON_VERSION"


# Build venv for slim Alpine variant
# Note: Alpine slim uses minimal extras to avoid complex native dependencies
# (e.g., spacy/blis which don't have pre-built musl wheels)
FROM alpine-build AS install-slim-alpine

COPY --chown=datahub ./metadata-ingestion /metadata-ingestion

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN test -d /metadata-ingestion/src/datahub/metadata
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1

RUN uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,s3-slim,gcs-slim,abs-slim]" && \
    datahub --version


# Build venv for locked Alpine variant
FROM alpine-build AS install-locked-alpine

COPY --chown=datahub ./metadata-ingestion /metadata-ingestion

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN test -d /metadata-ingestion/src/datahub/metadata
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1

RUN uv pip install -e "/metadata-ingestion/[base,datahub-rest,datahub-kafka,s3-slim,gcs-slim,abs-slim]" && \
    datahub --version


# Final Alpine slim variant
FROM alpine-base AS final-slim-alpine

COPY --from=install-slim-alpine --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv
COPY --from=install-slim-alpine --chown=datahub:datahub /home/datahub/.local/share/uv/python /home/datahub/.local/share/uv/python
COPY --chown=datahub ./metadata-ingestion /metadata-ingestion

USER datahub
WORKDIR $HOME

ENTRYPOINT [ "datahub" ]


# Final Alpine locked variant (network blocked)
FROM alpine-base AS final-locked-alpine

COPY --from=install-locked-alpine --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv
COPY --from=install-locked-alpine --chown=datahub:datahub /home/datahub/.local/share/uv/python /home/datahub/.local/share/uv/python
COPY --chown=datahub ./metadata-ingestion /metadata-ingestion

# Block network access to PyPI
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

USER datahub
WORKDIR $HOME

ENTRYPOINT [ "datahub" ]


# =============================================================================
# DEFAULT EXPORT - Use APP_ENV to select variant (defaults to full)
# =============================================================================

FROM final-${APP_ENV} AS final

ENTRYPOINT [ "datahub" ]
