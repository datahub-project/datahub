# datahub-ai-service: serves DataHub's MCP tools over HTTP at /mcp (port 9005).
#
# Third-party dependencies come from requirements.lock; the DataHub SDK,
# datahub-core-mcp-tools and datahub-ai-service are installed from source at the build commit.

ARG PYTHON_VERSION=3.11
ARG BASE_IMAGE=cgr.dev/chainguard/wolfi-base:latest

FROM ${BASE_IMAGE} AS base

ENV HOME=/home/datahub
USER root
RUN addgroup -g 1000 datahub && \
    adduser -D -u 1000 -G datahub -h /home/datahub -s /bin/sh datahub && \
    chmod g-s $HOME

ARG PYTHON_VERSION
RUN test -n "${PYTHON_VERSION}"
# Bump to pull fresh Wolfi packages (security fixes) without other changes.
ARG WOLFI_PKG_REFRESH=2026-10-27
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    echo "wolfi pkg refresh: ${WOLFI_PKG_REFRESH}" && \
    apk upgrade --no-cache --update-cache && \
    apk add --no-cache --update-cache ca-certificates python-${PYTHON_VERSION} uv && \
    ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python3 && \
    ln -sf /usr/bin/python3 /usr/bin/python

COPY ./docker/snippets/openssl4_ssl_compat.py /tmp/openssl4_ssl_compat.py
RUN python3 -c 'import pathlib, shutil, sysconfig; dest = pathlib.Path(sysconfig.get_path("stdlib")) / "sitecustomize.py"; shutil.copyfile("/tmp/openssl4_ssl_compat.py", dest)' \
 && rm /tmp/openssl4_ssl_compat.py

USER datahub
WORKDIR $HOME
ENV VIRTUAL_ENV=$HOME/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ARG UV_PROFILE=default
ARG DEFAULT_INDEX_URL=""
ARG EXTRA_INDEX_URLS=""
RUN --mount=type=bind,source=./docker/snippets/uv,target=/tmp/uv-config \
    mkdir -p $HOME/.config/uv && \
    if [ "${UV_PROFILE}" = "custom" ] || [ -n "${EXTRA_INDEX_URLS}" ]; then \
        PROFILES_DIR=/tmp/uv-config/profiles BASE_PROFILE="${UV_PROFILE}" UV_PROFILE="${UV_PROFILE}" DEFAULT_INDEX_URL="${DEFAULT_INDEX_URL}" EXTRA_INDEX_URLS="${EXTRA_INDEX_URLS}" \
          python3 /tmp/uv-config/generate-config.py > $HOME/.config/uv/uv.toml; \
    else \
        cp /tmp/uv-config/profiles/${UV_PROFILE}.toml $HOME/.config/uv/uv.toml; \
    fi

FROM base AS build

RUN uv venv --python "${PYTHON_VERSION}"

# Dependencies first, so source changes don't reinstall them.
COPY --chown=datahub:datahub ./docker/datahub-ai-service/requirements.lock /tmp/requirements.lock
RUN --mount=type=cache,target=/home/datahub/.cache/uv,uid=1000,gid=1000 \
    --mount=type=secret,id=netrc,target=/home/datahub/.netrc,required=false,uid=1000,gid=1000 \
    uv pip install -r /tmp/requirements.lock

COPY --chown=datahub:datahub ./metadata-ingestion /src/metadata-ingestion
COPY --chown=datahub:datahub ./packages/datahub-core-mcp-tools /src/datahub-core-mcp-tools
COPY --chown=datahub:datahub ./packages/datahub-ai-service /src/datahub-ai-service

# Stamp the release version into all three packages.
ARG RELEASE_VERSION
RUN test -n "${RELEASE_VERSION}" && \
    for f in /src/metadata-ingestion/src/datahub/_version.py \
             /src/datahub-core-mcp-tools/src/datahub_core_mcp_tools/_version.py \
             /src/datahub-ai-service/src/datahub_ai_service/_version.py; do \
      sed -i "s/__version__ = .*$/__version__ = \"${RELEASE_VERSION}\"/" "$f"; \
    done

# Building these source trees fetches their build requirements (setuptools, wheel) from the
# same index as the lock install above, so it needs the same credentials.
RUN --mount=type=cache,target=/home/datahub/.cache/uv,uid=1000,gid=1000 \
    --mount=type=secret,id=netrc,target=/home/datahub/.netrc,required=false,uid=1000,gid=1000 \
    uv pip install --no-deps /src/metadata-ingestion /src/datahub-core-mcp-tools /src/datahub-ai-service && \
    # The image scanner reads pip's vendored SBOM and reports packages that aren't installed.
    find $VIRTUAL_ENV -path '*/pip/_vendor/bom.cdx.json' -delete && \
    datahub-ai-service --version

# Only the virtualenv: the source trees it was built from stay behind.
FROM base AS final

COPY --from=build --chown=datahub:datahub /home/datahub/.venv /home/datahub/.venv

ENV DATAHUB_AI_HOST=0.0.0.0
ENV DATAHUB_AI_PORT=9005
EXPOSE 9005

# Python, not curl: the image has no curl.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python3", "-c", "import os, urllib.request; urllib.request.urlopen('http://localhost:' + os.environ.get('DATAHUB_AI_PORT', '9005') + '/health', timeout=4)"]

USER datahub
CMD ["datahub-ai-service", "serve"]
