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


# TODO: we may need more variants, but starting with slim for now.
FROM ingestion-base-slim AS final

# Install Node.js and Yarn and cypress System dependencies
USER 0
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
      libgtk2.0-0 \
      libgtk-3-0 \
      libgbm-dev \
      libnotify-dev \
      libnss3 \
      libxss1 \
      libasound2t64 \
      libxtst6 \
      xauth \
      xvfb \
    && npm install -g yarn \
    && rm -rf /var/lib/apt/lists/*


COPY --chown=datahub:datahub ./smoke-test/requirements-release-test.txt /smoke-test/requirements-release-test.txt
COPY --chown=datahub:datahub ./smoke-test/tests/cypress/package.json /smoke-test/tests/cypress/yarn.lock /smoke-test/tests/cypress/

USER datahub
WORKDIR /smoke-test
RUN UV_LINK_MODE=copy uv pip install -r requirements-release-test.txt

RUN yarn install --frozen-lockfile --cwd /smoke-test/tests/cypress

USER 0

WORKDIR /
COPY --chown=datahub:datahub ./smoke-test /smoke-test
COPY --chown=datahub:datahub ./docker/smoke-test/remote-smoke-test.sh /smoke-test/

USER datahub

WORKDIR /smoke-test

ENV DATAHUB_GMS_URL=""
ENV DATAHUB_FRONTEND_URL=""
ENV CYPRESS_BASE_URL=""

#CMD ["pytest", "-rP", "--durations=20", "-vvs", "--continue-on-collection-errors",  "tests/cypress/integration_test.py"]
CMD ["bash", "-c", "/smoke-test/remote-smoke-test.sh"]