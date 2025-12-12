# This image has two variants: full and slim.
# The full variant has additional deps preinstalled, like a JRE and Oracle client.
ARG APP_ENV=full
ARG PYTHON_VERSION=3.11

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
# We need to install build-essential in order to build some Python packages (e.g. python-ldap)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install --no-install-recommends -y -qq \
    default-jre-headless \
    build-essential

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
RUN test -n "$BUNDLED_CLI_VERSION"

# Create venv directory
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Copy the self-contained venv build scripts
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Make scripts executable
RUN chmod +x /tmp/build_bundled_venvs_unified.sh && \
    chmod +x /tmp/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (standard s3 with PySpark)
WORKDIR /tmp
RUN ./build_bundled_venvs_unified.sh

USER datahub

# =============================================================================
# PRE-BUILD BUNDLED INGESTION VENVS - SLIM VARIANT
# =============================================================================

FROM ingestion-base-slim AS bundled-venvs-slim
USER 0

# Set up bundled venv configuration for SLIM variant (without PySpark)
# Venv named s3-bundled but uses s3-slim package internally
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_VENV_SLIM_MODE="true"
ARG BUNDLED_CLI_VERSION
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_VENV_SLIM_MODE=${BUNDLED_VENV_SLIM_MODE}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}
RUN test -n "$BUNDLED_CLI_VERSION"

# Create venv directory
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Copy the self-contained venv build scripts
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Make scripts executable
RUN chmod +x /tmp/build_bundled_venvs_unified.sh && \
    chmod +x /tmp/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (s3-slim without PySpark)
WORKDIR /tmp
RUN ./build_bundled_venvs_unified.sh

USER datahub

# =============================================================================
# PRE-BUILD BUNDLED INGESTION VENVS - LOCKED VARIANT
# =============================================================================

FROM ingestion-base-slim AS bundled-venvs-locked
USER 0

# Set up bundled venv configuration for LOCKED variant (without PySpark, network blocked)
# Same as slim but will have network access disabled in final stage
ARG BUNDLED_VENV_PLUGINS="s3,demo-data"
ARG BUNDLED_VENV_SLIM_MODE="true"
ARG BUNDLED_CLI_VERSION
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV BUNDLED_VENV_SLIM_MODE=${BUNDLED_VENV_SLIM_MODE}
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}
RUN test -n "$BUNDLED_CLI_VERSION"

# Create venv directory
RUN mkdir -p $DATAHUB_BUNDLED_VENV_PATH && \
    chown -R datahub:datahub $DATAHUB_BUNDLED_VENV_PATH

# Copy metadata-ingestion source (needed to build wheels)
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Copy the self-contained venv build scripts
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.py /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/build_bundled_venvs_unified.sh /tmp/
COPY --chown=datahub:datahub ./docker/snippets/ingestion/constraints.txt ${DATAHUB_BUNDLED_VENV_PATH}/

# Make scripts executable
RUN chmod +x /tmp/build_bundled_venvs_unified.sh && \
    chmod +x /tmp/build_bundled_venvs_unified.py

USER datahub

# Build bundled venvs using our self-contained script (s3-slim without PySpark)
WORKDIR /tmp
RUN ./build_bundled_venvs_unified.sh

USER datahub

# =============================================================================
# END BUNDLED VENVS SECTION
# =============================================================================

# =============================================================================
# FINAL STAGE - FULL VARIANT (default, with PySpark, network enabled)
# =============================================================================

FROM ingestion-base-full AS final-full

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-full $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH

COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin
COPY --chown=datahub:datahub ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY --chown=datahub:datahub ./docker/datahub-actions/readiness-check.sh /readiness-check.sh

RUN chmod a+x /start_datahub_actions.sh && \
    mkdir -p /etc/datahub/actions && \
    mkdir -p /tmp/datahub/logs/actions/system && \
    chown -R datahub:datahub /etc/datahub /tmp/datahub

# Install a cacheble layer that installs external dependencies and does not get invalidated due to changes in ingestion or actions code
# Copy just enough to enable pip compile to work. Other code changes wont invalidate this layer.
COPY --chown=datahub:datahub ./metadata-ingestion/setup.py /metadata-ingestion/
COPY --chown=datahub:datahub ./metadata-ingestion/src/datahub/_version.py /metadata-ingestion/src/datahub/
COPY --chown=datahub:datahub ./datahub-actions/setup.py /datahub-actions/
COPY --chown=datahub:datahub ./datahub-actions/src/datahub_actions/_version.py /datahub-actions/src/datahub_actions/
COPY --chown=datahub:datahub ./datahub-actions/README.md /datahub-actions/

USER datahub
RUN echo "-e /metadata-ingestion/ \n -e /datahub-actions/[all]" | uv pip compile /dev/stdin | grep -v "\-e" | uv pip install -r /dev/stdin
USER 0

COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion
COPY --chown=datahub:datahub ./datahub-actions /datahub-actions
COPY --chown=datahub:datahub ./docker/datahub-actions/config /etc/datahub/actions/system/conf

USER datahub

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION" # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1 && \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install metadata-ingestion with base extras (network enabled, can install more at runtime)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions \
  uv pip install -e '/metadata-ingestion/[base,s3,gcs,abs]'

# Install datahub-actions with all extras
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions \
  uv pip install -e '/datahub-actions/[all]'

ENTRYPOINT [ ]
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh

# =============================================================================
# FINAL STAGE - SLIM VARIANT (no PySpark, network enabled)
# =============================================================================

FROM ingestion-base-slim AS final-slim

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-slim $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH

COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin
COPY --chown=datahub:datahub ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY --chown=datahub:datahub ./docker/datahub-actions/readiness-check.sh /readiness-check.sh

RUN chmod a+x /start_datahub_actions.sh && \
    mkdir -p /etc/datahub/actions && \
    mkdir -p /tmp/datahub/logs/actions/system && \
    chown -R datahub:datahub /etc/datahub /tmp/datahub

# Install a cacheable layer that installs external dependencies
COPY --chown=datahub:datahub ./metadata-ingestion/setup.py /metadata-ingestion/
COPY --chown=datahub:datahub ./metadata-ingestion/src/datahub/_version.py /metadata-ingestion/src/datahub/
COPY --chown=datahub:datahub ./datahub-actions/setup.py /datahub-actions/
COPY --chown=datahub:datahub ./datahub-actions/src/datahub_actions/_version.py /datahub-actions/src/datahub_actions/
COPY --chown=datahub:datahub ./datahub-actions/README.md /datahub-actions/

USER datahub
RUN echo "-e /metadata-ingestion/ \n -e /datahub-actions/[all]" | uv pip compile /dev/stdin | grep -v "\-e" | uv pip install -r /dev/stdin
USER 0

COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion
COPY --chown=datahub:datahub ./datahub-actions /datahub-actions
COPY --chown=datahub:datahub ./docker/datahub-actions/config /etc/datahub/actions/system/conf

USER datahub

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1 && \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install metadata-ingestion with SLIM extras (no PySpark, network enabled for flexibility)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions \
  uv pip install -e '/metadata-ingestion/[base,s3-slim,gcs-slim,abs-slim]'

# Install datahub-actions with all extras
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions \
  uv pip install -e '/datahub-actions/[all]'

ENTRYPOINT [ ]
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh

# =============================================================================
# FINAL STAGE - LOCKED VARIANT (no PySpark, network BLOCKED, bundled venvs only)
# =============================================================================

FROM ingestion-base-slim AS final-locked

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-locked $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH

COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin
COPY --chown=datahub:datahub ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY --chown=datahub:datahub ./docker/datahub-actions/readiness-check.sh /readiness-check.sh

RUN chmod a+x /start_datahub_actions.sh && \
    mkdir -p /etc/datahub/actions && \
    mkdir -p /tmp/datahub/logs/actions/system && \
    chown -R datahub:datahub /etc/datahub /tmp/datahub

# NO metadata-ingestion install in locked variant - only bundled venvs available
# This ensures complete isolation and prevents any package installations

# Copy only datahub-actions code (not metadata-ingestion)
COPY --chown=datahub:datahub ./datahub-actions/setup.py /datahub-actions/
COPY --chown=datahub:datahub ./datahub-actions/src/datahub_actions/_version.py /datahub-actions/src/datahub_actions/
COPY --chown=datahub:datahub ./datahub-actions/README.md /datahub-actions/

USER datahub
# Install only datahub-actions, NOT metadata-ingestion
RUN echo "-e /datahub-actions/[all]" | uv pip compile /dev/stdin | grep -v "\-e" | uv pip install -r /dev/stdin
USER 0

COPY --chown=datahub:datahub ./datahub-actions /datahub-actions
COPY --chown=datahub:datahub ./docker/datahub-actions/config /etc/datahub/actions/system/conf

USER datahub

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install ONLY datahub-actions (not metadata-ingestion)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions \
  uv pip install -e '/datahub-actions/[all]'

# Block network access to PyPI - locked variant only uses bundled venvs
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

ENTRYPOINT [ ]
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh

# =============================================================================
# DEFAULT EXPORT - Use APP_ENV to select variant (defaults to full)
# =============================================================================

FROM final-${APP_ENV} AS final
