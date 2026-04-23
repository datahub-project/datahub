# full / slim / locked variants (see APP_ENV). Base OS: Wolfi (Chainguard); override via WOLFI_BASE_IMAGE.
# The full variant has additional deps preinstalled, like a JRE and Oracle client.
ARG APP_ENV=full
ARG PYTHON_VERSION=3.11
ARG WOLFI_BASE_IMAGE=cgr.dev/chainguard/wolfi-base:latest

# INLINE-BEGIN @/docker/snippets/ingestion_base (Wolfi)
# Stages:
# - python-base: python, uv, venv, user datahub (UID 1000)
# - ingestion-base-slim: LDAP/SASL/Kerberos, librdkafka, ODBC, etc.
# - ingestion-base-full: JRE, build toolchain, Oracle Instant Client

FROM ${WOLFI_BASE_IMAGE} AS python-base

ENV HOME=/home/datahub
USER root

RUN addgroup -g 1000 datahub && \
    adduser -D -u 1000 -G datahub -h /home/datahub -s /bin/sh datahub && \
    mkdir -p $HOME && \
    chown -R datahub:datahub $HOME && \
    chmod g-s $HOME

ARG PYTHON_VERSION
RUN test -n "${PYTHON_VERSION}"  # PYTHON_VERSION must be set

RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    apk add --no-cache --update-cache \
    ca-certificates \
    python-${PYTHON_VERSION} \
    python-${PYTHON_VERSION}-dev \
    uv \
    git \
    wget \
    curl \
    zip \
    unzip \
    bash

RUN ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python3 && \
    ln -sf /usr/bin/python3 /usr/bin/python

ARG PIP_MIRROR_URL=https://pypi.python.org/simple
ARG PIP_EXTRA_INDEX_URL=""
ENV PIP_INDEX_URL=${PIP_MIRROR_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
ENV UV_INDEX_URL=${PIP_MIRROR_URL}
RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then uvx --no-cache pip config set global.index-url ${PIP_MIRROR_URL} ; fi && \
    if [ -n "${PIP_EXTRA_INDEX_URL}" ] ; then uvx --no-cache pip config set global.extra-index-url ${PIP_EXTRA_INDEX_URL} ; fi

USER datahub
WORKDIR $HOME
RUN uv venv --python "${PYTHON_VERSION}"
ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Use the system CA bundle. uv: https://github.com/astral-sh/uv/issues/1474
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"

FROM python-base AS ingestion-base-slim

USER 0
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    apk add --no-cache --update-cache \
    openldap-2.6-dev \
    cyrus-sasl-dev \
    cyrus-sasl-libs \
    krb5-dev \
    krb5-libs \
    libaio-dev \
    libaio \
    librdkafka-dev \
    openldap-2.6-clients \
    unixodbc \
    unixodbc-dev
USER datahub

FROM ingestion-base-slim AS ingestion-base-full

USER 0
# build-base pulls gcc (includes g++ driver), make, binutils, glibc-dev on Wolfi; there is no separate g++ apk.
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    apk add --no-cache --update-cache \
    openjdk-21-jre \
    build-base

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
ARG BUNDLED_VENV_PLUGINS="s3,demo-data,file,datahub-gc,datahub-documents"
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
ARG BUNDLED_VENV_PLUGINS="s3,demo-data,file,datahub-gc,datahub-documents"
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
ARG BUNDLED_VENV_PLUGINS="s3,demo-data,file,datahub-gc,datahub-documents"
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

ENV UV_BUILD_CONSTRAINT=/metadata-ingestion/build-constraints.txt

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION" # RELEASE_VERSION is a required build arg
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1 && \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install metadata-ingestion with base extras (network enabled, can install more at runtime)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions,sharing=private \
  uv pip install -e '/metadata-ingestion/[base,s3,gcs,abs]'

# Install datahub-actions with all extras
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions,sharing=private \
  uv pip install -e '/datahub-actions/[all]'

ENTRYPOINT [ ]
CMD ["/bin/bash", "/start_datahub_actions.sh"]

# =============================================================================
# FINAL STAGE - SLIM VARIANT (no PySpark, network enabled)
# =============================================================================

FROM ingestion-base-slim AS final-slim

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-slim $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH

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

ENV UV_BUILD_CONSTRAINT=/metadata-ingestion/build-constraints.txt

ARG RELEASE_VERSION
RUN test -n "$RELEASE_VERSION"
RUN --mount=type=bind,source=./python-build/version_updater.py,target=/version_updater.py \
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1 && \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install metadata-ingestion with SLIM extras (no PySpark, network enabled for flexibility)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions,sharing=private \
  uv pip install -e '/metadata-ingestion/[base,s3-slim,gcs-slim,abs-slim]'

# Install datahub-actions with all extras
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions,sharing=private \
  uv pip install -e '/datahub-actions/[all]'

ENTRYPOINT [ ]
CMD ["/bin/bash", "/start_datahub_actions.sh"]

# =============================================================================
# FINAL STAGE - LOCKED VARIANT (no PySpark, network BLOCKED, bundled venvs only)
# =============================================================================

FROM ingestion-base-slim AS final-locked

USER root

ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs
COPY --from=bundled-venvs-locked $DATAHUB_BUNDLED_VENV_PATH $DATAHUB_BUNDLED_VENV_PATH

COPY --chown=datahub:datahub ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY --chown=datahub:datahub ./docker/datahub-actions/readiness-check.sh /readiness-check.sh

RUN chmod a+x /start_datahub_actions.sh && \
    mkdir -p /etc/datahub/actions && \
    mkdir -p /tmp/datahub/logs/actions/system && \
    chown -R datahub:datahub /etc/datahub /tmp/datahub

# Bundled venvs under /opt/datahub/venvs are built with editable installs against
# /metadata-ingestion. Ship that tree so subprocess ingestion can import datahub;
# the default app venv still does not pip-install metadata-ingestion (PyPI blocked below).
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

# Copy only datahub-actions code for the actions framework venv (not a pip install of metadata-ingestion)
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
    python /version_updater.py --directory /metadata-ingestion/ --version "$RELEASE_VERSION" --expected-update-count 1 && \
    python /version_updater.py --directory /datahub-actions/ --version "$RELEASE_VERSION" --expected-update-count 1

# Install ONLY datahub-actions into the default venv (not metadata-ingestion)
RUN --mount=type=cache,target=$HOME/.cache/uv,uid=1000,gid=1000,id=datahub-actions,sharing=private \
  uv pip install -e '/datahub-actions/[all]'

# Block network access to PyPI - locked variant has no runtime pip installs for ingestion CLI
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple

ENTRYPOINT [ ]
CMD ["/bin/bash", "/start_datahub_actions.sh"]

# =============================================================================
# DEFAULT EXPORT - Use APP_ENV to select variant (defaults to full)
# =============================================================================

FROM final-${APP_ENV} AS final
