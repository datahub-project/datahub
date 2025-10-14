# DataHub Executor Images: Full (default) and Slim

## Overview

There are two published variants of the DataHub Executor image built from docker/datahub-executor/Dockerfile:

- Default (full): includes prebuilt ingestion virtualenvs (“bundled venvs”) for a small default plugin set. Tagged as `<tag>` (no suffix).
- Slim: contains no prebuilt venvs. It keeps the unified venv builder scripts inside the image so you can add your own plugin venvs at build time. Tagged as `<tag>-slim`.

Important: If you need to build your own plugin venvs, always start FROM the slim image. The default (full) image is intended for direct use and may not contain or expose everything required for extending with custom venvs in some distributions/environments.

The builder creates per‑plugin virtualenvs under `/opt/datahub/venvs` using predictable names: `<plugin>-bundled`.

## What’s inside the slim image

- `build_bundled_venvs_unified.sh` and `build_bundled_venvs_unified.py` are installed at `/usr/local/bin/` and kept in the image.
- `uv` and Python runtime are present.
- The venv root `/opt/datahub/venvs` is created and owned by the `datahub` user.
- No virtualenvs are built by default.

## Tags and naming

- Executor repository: `795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor`
- Default (full) image tag: `<tag>` (e.g., `head`, `v0.3.14.2`)
- Slim image tag: `<tag>-slim`

Examples:

- `.../datahub-executor:head` (full)
- `.../datahub-executor:head-slim` (slim)

## Publishing (for maintainers)

Build and push both variants. Note: `RELEASE_VERSION` is required by the Dockerfile; set appropriately.

```bash
# Full (default) — prebuilt venvs
docker buildx build \
  --build-arg APP_ENV=prod \
  --build-arg RELEASE_VERSION=1.0.0 \
  --build-arg BUILD_BUNDLED_VENVS=true \
  --platform linux/amd64 \
  -t 795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:1.0.0 \
  -f docker/datahub-executor/Dockerfile \
  --push .

# Slim — no prebuilt venvs
docker buildx build \
  --build-arg APP_ENV=prod \
  --build-arg RELEASE_VERSION=1.0.0 \
  --build-arg BUILD_BUNDLED_VENVS=false \
  --platform linux/amd64 \
  -t 795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:1.0.0-slim \
  -f docker/datahub-executor/Dockerfile \
  --push .
```

## Customer instructions: Extend the slim image and add your plugins

Use only the slim variant as your base image. You don’t need this repository to build your derived image—just the slim image reference.

Create a Dockerfile like the following (copy/paste). This template parameterizes the base image (`FROM`) so you can inject the exact tag you want at build time:

```dockerfile
# Always start from the slim executor image
ARG BASE_IMAGE=795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:<tag>-slim
FROM ${BASE_IMAGE}

# Choose the DataHub CLI version and the plugin list you want to bundle
ARG BUNDLED_CLI_VERSION=1.2.0.10
ARG BUNDLED_VENV_PLUGINS=snowflake,oracle,tableau,s3,kafka

# Expose values to the builder script
ENV BUNDLED_CLI_VERSION=${BUNDLED_CLI_VERSION}
ENV BUNDLED_VENV_PLUGINS=${BUNDLED_VENV_PLUGINS}
ENV DATAHUB_BUNDLED_VENV_PATH=/opt/datahub/venvs

# Build the per‑plugin virtualenvs using the script inside the slim image
USER datahub
RUN /usr/local/bin/build_bundled_venvs_unified.sh
```

Build your derived image (replace args as needed). Note the base image points to `:*-slim`:

```bash
# If the base is in private ECR, first log in:
# aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 795586375822.dkr.ecr.us-west-2.amazonaws.com

docker build \
  --build-arg BASE_IMAGE=795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:1.0.0-slim \
  --build-arg BUNDLED_CLI_VERSION=1.2.0.10 \
  --build-arg BUNDLED_VENV_PLUGINS=bigquery,redshift \
  -t customer-org/datahub-executor:with-plugins \
  .
```

## Air-gapped or mirrored PyPI builds (optional)

If your environment cannot reach public PyPI, you can point pip to your internal indexes during build. Either add these lines to your Dockerfile or pass them as build args.

```dockerfile
# In your Dockerfile (optional block)
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
```

When building, pass your mirror URLs:

```bash
docker build \
  --build-arg BASE_IMAGE=795586375822.dkr.ecr.us-west-2.amazonaws.com/datahub-executor:1.0.0-slim \
  --build-arg BUNDLED_CLI_VERSION=1.2.0.10 \
  --build-arg BUNDLED_VENV_PLUGINS=bigquery,redshift \
  --build-arg PIP_INDEX_URL=https://pypi.your-internal.mirror/simple \
  --build-arg PIP_EXTRA_INDEX_URL=https://secondary.mirror/simple \
  -t customer-org/datahub-executor:with-plugins \
  .
```

Note: If you don’t specify `PIP_INDEX_URL`/`PIP_EXTRA_INDEX_URL`, the build will use public PyPI. These parameters are only needed for air‑gapped or mirrored builds.

Verify venvs (optional):

```bash
docker run --rm customer-org/datahub-executor:with-plugins ls -1 /opt/datahub/venvs
```

## Runtime usage (example)

Run the executor container as usual. The image will include the venvs you built during the customer docker build stage.

```bash
docker run --rm \
  -e DATAHUB_GMS_HOST=gms.yourdomain \
  -e DATAHUB_GMS_PORT=8080 \
  customer-org/datahub-executor:with-plugins
```

## Notes and tips

- Plugins list is a comma‑separated list matching `acryl-datahub` extras (e.g., `snowflake`, `bigquery`, `redshift`, `s3`, `kafka`, `oracle`, `tableau`, etc.).
- The builder accepts `BUNDLED_CLI_VERSION` with or without a leading `v` (e.g., `v1.2.0.10` or `1.2.0.10`).
- For air‑gapped environments, point pip to your internal index before `RUN`, for example:

```dockerfile
ENV PIP_INDEX_URL=https://pypi.your-internal.mirror/simple
ENV PIP_EXTRA_INDEX_URL=https://additional-mirror/simple
```

- Multi‑arch builds: add `--platform linux/amd64,linux/arm64` to `docker buildx build` if you plan to publish multi‑arch images.
- Summary: Use `datahub-executor:<tag>-slim` as your base when building custom plugin venvs. The default `:<tag>` image is for direct use and not intended as a base for further venv bundling.

## Troubleshooting / FAQ

- Why does the build complain that `build_bundled_venvs_unified.py` is missing?
  - Ensure you are extending the slim image: it contains the builder scripts at `/usr/local/bin`. If you start FROM the full image, the required tools may be missing or not intended for reuse.
  - To verify inside a built image:
    - `docker run --rm -it <your-image> bash -lc 'ls -l /usr/local/bin/build_bundled_venvs_unified.*'`
- Is this related to test vs actual build?
  - A unit/CI test may execute scripts on the host filesystem, where `/usr/local/bin/build_bundled_venvs_unified.py` does not exist. The Docker image contains the scripts. For tests that need to execute the builder, run inside the image/container or reference the repo path version `docker/snippets/ingestion/build_bundled_venvs_unified.py`.
- Do I need to set `PIP_INDEX_URL`/`PIP_EXTRA_INDEX_URL`?
  - No for standard internet-connected builds (defaults to public PyPI). Yes only for air-gapped/mirrored environments, and they should be provided at build time so dependencies are fetched during the image build.
