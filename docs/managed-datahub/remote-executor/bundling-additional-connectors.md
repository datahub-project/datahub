# Bundling Additional Connectors

To add connectors that aren't already bundled in your Remote Executor image, build a custom image that bundles the extra connector(s). The example Dockerfile below starts from an **unlocked** `datahub-executor` image and bundles the connectors you list. The image this produces is bundled but not locked, so you can keep extending it; if you need a hardened image that cannot install packages at runtime, see [Optional: lock the image](#optional-lock-the-image).

## Build

```bash
docker build \
  --build-arg EXECUTOR_IMAGE=<registry>/datahub-executor:<tag>-slim \
  --build-arg BUNDLED_CLI_VERSION=<acryl-datahub version, e.g. 1.6.0.3> \
  --build-arg BUNDLED_VENV_PLUGINS=mysql,snowflake \
  -f Dockerfile.bundled .
```

Build args:

- **`EXECUTOR_IMAGE`** — the **unlocked** executor image to build on (`:tag` or `:tag-slim`); not `:tag-locked`.
- **`BUNDLED_CLI_VERSION`** — the `acryl-datahub` version to install the connectors at (typically your executor's version).
- **`BUNDLED_VENV_PLUGINS`** — comma-separated connectors to add.

Each connector you list gets its own bundled environment; a connector already present in the base image is replaced, and everything else in the image is left unchanged. To bundle from a private/authenticated package index, add a BuildKit secret mount for your credentials to the bundle `RUN` step.

## Dockerfile

<!-- prettier-ignore-start -->
```dockerfile
# syntax=docker/dockerfile:1

ARG EXECUTOR_IMAGE
FROM ${EXECUTOR_IMAGE}

USER root

ARG BUNDLED_CLI_VERSION
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}

ARG BUNDLED_VENV_PLUGINS
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}

RUN /usr/local/bin/build_bundled_venvs_unified.sh

USER datahub

```
<!-- prettier-ignore-end -->

## Optional: lock the image

Locking removes `pip`/`uv` and points the package indexes at an unreachable address, so no packages can be installed at runtime — useful for hardened or air-gapped deployments.

To lock, append these lines to the Dockerfile above (after the bundle `RUN`):

```dockerfile
USER root
RUN sh /usr/local/bin/lock_image.sh
ENV UV_INDEX_URL=http://127.0.0.1:1/simple
ENV UV_DEFAULT_INDEX=http://127.0.0.1:1/simple
ENV PIP_INDEX_URL=http://127.0.0.1:1/simple
ENV PIP_EXTRA_INDEX_URL=""
ENV UV_EXTRA_INDEX_URL=""
USER datahub
```
