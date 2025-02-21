# Defining environment
ARG APP_ENV=prod
ARG BASE_IMAGE=acryldata/datahub-ingestion
ARG DOCKER_VERSION=head-slim
# ARG DEBIAN_REPO_URL=https://deb.debian.org/debian
# ARG PIP_MIRROR_URL=https://pypi.python.org/simple

FROM powerman/dockerize:0.19 AS dockerize-binary

FROM $BASE_IMAGE:$DOCKER_VERSION AS build
USER 0

# # Optionally set corporate mirror for deb
# ARG DEBIAN_REPO_URL
# RUN if [ "${DEBIAN_REPO_URL}" != "http://deb.debian.org/debian" ] ; then sed -i "s#http.*://deb.debian.org/debian#${DEBIAN_REPO_URL}#g" /etc/apt/sources.list.d/debian.sources ; fi
# ENV DEBIAN_FRONTEND noninteractive

# # Optionally set corporate mirror for pip
# ARG PIP_MIRROR_URL
# RUN if [ "${PIP_MIRROR_URL}" != "https://pypi.python.org/simple" ] ; then pip config set global.index-url ${PIP_MIRROR_URL} ; fi
# ENV UV_INDEX_URL=${PIP_MIRROR_URL}

COPY --from=dockerize-binary /usr/local/bin/dockerize /usr/local/bin

# Create a symbolic link for CA certificates. This is a workaround
# to make duckdb work with the CA certificates in the image.
RUN mkdir -p /etc/pki/tls/certs && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

# Install required runtime dependencies 
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    libc6 \
    libstdc++6 \
    zlib1g \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install DuckDB with extensions
RUN DUCKDB_VERSION=v1.2.0 && \
    # Get system architecture
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        DUCKDB_ARCH="aarch64"; \
    else \
        DUCKDB_ARCH="amd64"; \
    fi && \
    # Download and extract DuckDB
    mkdir -p /tmp/duckdb && \
    cd /tmp/duckdb && \
    wget -q https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-${DUCKDB_ARCH}.zip && \
    unzip -q duckdb_cli-linux-${DUCKDB_ARCH}.zip && \
    chmod +x duckdb && \
    mv duckdb /usr/local/bin/ && \
    # Clean up
    cd / && \
    rm -rf /tmp/duckdb && \
    chown -R datahub /datahub-ingestion


COPY --chmod=755 ./docker/datahub-integrations-service/start.sh /start_datahub_integrations_service.sh

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV VIRTUAL_ENV=/datahub-ingestion/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

COPY --chown=datahub:datahub ./datahub-integrations-service /datahub-integrations-service
COPY --chown=datahub:datahub ./metadata-ingestion /metadata-ingestion

WORKDIR /datahub-integrations-service

USER datahub
# Initial extension installation (will be cached)
RUN duckdb -c "INSTALL httpfs; INSTALL aws;"

# Note that the requirements file contains editable installs for
# metadata-ingestion and datahub-integrations-service
RUN --mount=type=cache,target=/datahub-ingestion/.cache/uv,uid=1000,gid=1000 \
   UV_LINK_MODE=copy uv pip sync requirements.txt

# Production Image Built From Build Image
FROM build AS prod-install

# For the production build, we build a wheel file statically.
# In development mode, we build the file on-the-fly.
RUN python -m build --wheel . --outdir static/prebuilt-dist/ && \
    python -m build --wheel ../metadata-ingestion --outdir static/prebuilt-dist/

FROM build AS dev-install

# Dummy stage for development. Assumes most of the code is built on your machine and mounted to this image.
# The behavior is a bit tricky - we're mounting dev code over the code that was baked into the image
# as part of the build step.
# See this excellent thread https://github.com/docker/cli/issues/1134

# Bring in DataHub Actions
# COPY ./datahub-actions/datahub-actions/setup.py /datahub-actions/datahub-actions/
# COPY ./datahub-actions/datahub-actions/README.md /datahub-actions/datahub-actions/
# COPY ./datahub-actions/datahub-actions/src/datahub_actions/__init__.py /datahub-actions/datahub-actions/src/datahub_actions/__init__.py
ENV EXTRA_UVICORN_ARGS="--reload --reload-dir /datahub-integrations-service/src --reload-dir /metadata-ingestion/src"

FROM ${APP_ENV}-install AS final

HEALTHCHECK --start-period=2m --retries=4 CMD curl --fail http://localhost:9003/ping || exit 1
ENTRYPOINT []
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_integrations_service.sh