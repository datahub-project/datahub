# Bundling Additional Connectors

To add connectors that aren't already bundled in your Remote Executor image, build a custom image that bundles the extra connector(s). The example Dockerfile below starts from an **unlocked** `datahub-executor` image and bundles the connectors you list. The image this produces is bundled but not locked, so you can keep extending it; if you need a hardened image that cannot install packages at runtime, see [Optional: lock the image](#optional-lock-the-image).

## Example: Dockerfile.bundled

Save the following as **`Dockerfile.bundled`** in your build context (for example, the current directory). Do not name it `Dockerfile` unless you also change the `-f` flag in the build command below.

<!-- prettier-ignore-start -->
```dockerfile
{{ inline /docker/examples/Dockerfile.bundled }}
```
<!-- prettier-ignore-end -->

## Build

```bash
docker build \
  --platform linux/amd64 \
  --build-arg EXECUTOR_IMAGE=<registry>/datahub-executor:<tag>-slim \
  --build-arg BUNDLED_VENV_PLUGINS=mysql,snowflake \
  -f Dockerfile.bundled .
```

Set **`--platform`** for a single-architecture image (the example uses `linux/amd64`, the common cloud default). For mixed amd64/arm64 fleets, see [multi-arch](#multi-arch-image) under [CPU architecture](#cpu-architecture).

Build args:

- **`EXECUTOR_IMAGE`** — the **unlocked** executor image to build on (`:tag` or `:tag-slim`); not `:tag-locked`.
- **`BUNDLED_VENV_PLUGINS`** — comma-separated connectors to add.
- **`BUNDLED_CLI_VERSION`** _(optional)_ — override the `acryl-datahub` pin. Defaults to the base image's `BUNDLED_CLI_VERSION` (or the installed `acryl-datahub` version if that ENV is unset). Omit this unless you intentionally want a different CLI than the executor image.

Each connector you list gets its own bundled environment; a connector already present in the base image is replaced, and everything else in the image is left unchanged. To bundle from a private/authenticated package index, add a BuildKit secret mount for your credentials to the bundle `RUN` step.

### CPU architecture

Bundled venvs install architecture-specific Python wheels and native libraries. The image you run must contain venvs built for the **same CPU architecture** as the nodes that run the Remote Executor (or `datahub-actions`). Mixing arm64-built venvs onto amd64 nodes (or the reverse) causes import or binary failures.

#### Single-platform image

Build one architecture that matches your deployment. Pass a single **`--platform`** and an **`EXECUTOR_IMAGE`** that resolves to that arch (an arch-specific tag, or a multi-arch base that pulls the matching variant under `--platform`).

```bash
# amd64 cluster (common cloud default); also use this when building on Apple Silicon for amd64 nodes
docker build \
  --platform linux/amd64 \
  --build-arg EXECUTOR_IMAGE=<registry>/datahub-executor:<tag>-slim \
  --build-arg BUNDLED_VENV_PLUGINS=mysql,snowflake \
  -f Dockerfile.bundled \
  -t <registry>/datahub-executor-custom:<tag> \
  .

# arm64 cluster / arm64-only nodes
docker build \
  --platform linux/arm64 \
  --build-arg EXECUTOR_IMAGE=<registry>/datahub-executor:<tag>-slim \
  --build-arg BUNDLED_VENV_PLUGINS=mysql,snowflake \
  -f Dockerfile.bundled \
  -t <registry>/datahub-executor-custom:<tag> \
  .
```

Verify before deploying:

```bash
docker image inspect <registry>/datahub-executor-custom:<tag> --format '{{.Architecture}}'
```

#### Multi-arch image

If you run the executor on a mix of amd64 and arm64 nodes (or want one tag that works on both), build and push a **multi-arch** image with Buildx. Each platform gets its own layer set and its own bundled venvs; the registry stores a manifest list that clients pull by node architecture.

Requirements:

- A base **`EXECUTOR_IMAGE`** that itself is multi-arch (or that you can resolve per platform).
- Docker Buildx with QEMU/binfmt if you cross-build (for example building `linux/amd64` on Apple Silicon).

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg EXECUTOR_IMAGE=<registry>/datahub-executor:<tag>-slim \
  --build-arg BUNDLED_VENV_PLUGINS=mysql,snowflake \
  -f Dockerfile.bundled \
  -t <registry>/datahub-executor-custom:<tag> \
  --push \
  .
```

`--push` is required for multi-platform manifests to land in the registry. Prefer a multi-arch tag when your fleet is mixed; use a single-platform build when every node shares one architecture (simpler and faster).

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

## Using a bundled connector

Building the image only makes the venv available — a run actually uses it only when its **CLI version** is set to **`bundled`**
(the `version` execution argument; defaults to `latest`). Set it on the ingestion source in the DataHub UI under **Advanced → CLI
Version**:

- **`bundled`** → run from the pre-built `/opt/datahub/venvs/{plugin}-bundled` (no runtime dependency install or associated
  ephemeral-storage growth).
- empty (default), **`latest`**, or a specific `acryl-datahub` version → build a dynamic venv at runtime.

If the connector isn't bundled in the image, the run falls back to a dynamic venv — and on a **locked** image (no `uv`/`pip`, or no
reachable package index) that fallback fails. See
[Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md#using-a-bundled-venv) for the full picture.
