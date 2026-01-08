# DataHub Executor Images

## Overview

The DataHub Executor image is built from `docker/datahub-executor/Dockerfile` with multiple variants optimized for different use cases.

### Image Variants

| Variant          | Base OS       | Bundled Venvs      | Tag Suffix       | Use Case                       |
| ---------------- | ------------- | ------------------ | ---------------- | ------------------------------ |
| `full` (default) | Ubuntu 24.04  | ✅ Yes             | (none)           | Maximum compatibility          |
| `slim`           | Ubuntu 24.04  | ❌ No              | `-slim`          | Extend with custom plugins     |
| `locked-wolfi`   | Wolfi (glibc) | ✅ Yes             | `-locked-wolfi`  | Security-focused, minimal CVEs |
| `slim-alpine`    | Alpine 3.22   | ❌ No              | `-slim-alpine`   | Smallest footprint             |
| `locked-alpine`  | Alpine 3.22   | ⚠️ NOT IMPLEMENTED | `-locked-alpine` | (Bundled venvs stage missing)  |

### Tag Format

```
datahub-executor:<tag>                 # full (Ubuntu, with bundled venvs)
datahub-executor:<tag>-slim            # slim (Ubuntu, no bundled venvs)
datahub-executor:<tag>-locked-wolfi    # wolfi (Wolfi, with bundled venvs)
datahub-executor:<tag>-slim-alpine     # alpine (Alpine, no bundled venvs)
```

### Wolfi Variant (`locked-wolfi`)

The Wolfi variant uses [Chainguard's Wolfi](https://wolfi.dev/) as the base OS:

- **glibc-based**: Full compatibility with Python packages requiring native extensions (prophet, spacy, etc.)
- **Minimal CVE surface**: Rolling updates with rapid security patches
- **Bundled venvs**: Pre-built plugin virtualenvs at `/opt/datahub/venvs`
- **Locked PyPI**: Runtime `pip install` disabled for air-gapped/security-sensitive environments

### Alpine Variants (Limited Support)

Alpine uses musl libc which has known incompatibilities:

- `slim-alpine`: Available for extending with custom plugins (no bundled venvs)
- `locked-alpine`: **NOT IMPLEMENTED** - bundled venvs stage missing due to musl compatibility issues with some Python packages

### Choosing the Right Variant

- **`full`**: Maximum compatibility, all pre-built plugins included
- **`slim`**: Start here to build custom plugin sets (Ubuntu-based)
- **`locked-wolfi`**: Security-focused production deployments with pre-built plugins
- **`slim-alpine`**: Smallest image size when building custom plugins

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

### Using GitHub Actions Workflow (Recommended)

The `build-datahub-executor-bundled.yml` workflow builds and publishes images with customizable plugin bundles.

**Workflow Inputs:**

| Input             | Type    | Default                   | Description                               |
| ----------------- | ------- | ------------------------- | ----------------------------------------- |
| `build_wolfi`     | boolean | `true`                    | Build Wolfi variant (`-locked-wolfi`)     |
| `build_ubuntu`    | boolean | `true`                    | Build Ubuntu variant (`-slim`)            |
| `build_alpine`    | boolean | `false`                   | Build Alpine variant (NOT IMPLEMENTED)    |
| `plugins`         | string  | `s3,demo-data,looker,...` | Comma-separated plugin list               |
| `tag_override`    | string  | (empty)                   | Override tag (defaults to branch/release) |
| `push_ecr`        | boolean | `false`                   | Push to ECR                               |
| `push_cloudsmith` | boolean | `false`                   | Push to Cloudsmith                        |

**Example: Build wolfi with only S3 plugin**

1. Go to Actions → "Build and Publish Bundled VENV DataHub Executor"
2. Click "Run workflow"
3. Set:
   - `build_wolfi`: ✅
   - `build_ubuntu`: ❌
   - `plugins`: `s3`
   - `push_cloudsmith`: ✅

**Output:** `docker.cloudsmith.io/datahub/re/datahub-executor:<tag>-locked-wolfi`

### Using Gradle (Local Development)

Build specific variants locally:

```bash
# Wolfi variant with custom plugins
./gradlew :datahub-executor:docker \
  -PdatahubExecutorVariant=locked-wolfi \
  -PbundledVenvPlugins=s3,bigquery

# Ubuntu slim variant
./gradlew :datahub-executor:docker \
  -PdatahubExecutorVariant=slim \
  -PbundledVenvPlugins=snowflake,redshift
```

### Using Docker Buildx (Manual)

Build and push variants manually. Note: `RELEASE_VERSION` is required by the Dockerfile.

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

## Runtime package installation (optional)

You can optionally install additional Python packages at container startup using environment variables. This is useful for adding packages without rebuilding the image.

| Variable                                 | Description                                                                          | Default |
| ---------------------------------------- | ------------------------------------------------------------------------------------ | ------- |
| `DATAHUB_EXECUTOR_EXTRA_PACKAGES`        | Space-separated list of packages to install at startup                               | (empty) |
| `DATAHUB_EXECUTOR_EXTRA_PACKAGES_STRICT` | If `true`, fail startup on install error; if `false`/unset, log warning and continue | `false` |

**Examples:**

```bash
# Install packages, warn on failure (default behavior)
docker run --rm \
  -e DATAHUB_GMS_HOST=gms.yourdomain \
  -e DATAHUB_GMS_PORT=8080 \
  -e DATAHUB_EXECUTOR_EXTRA_PACKAGES="observe-models==1.0.0" \
  datahub-executor:latest

# Install packages, fail on error (strict mode)
docker run --rm \
  -e DATAHUB_GMS_HOST=gms.yourdomain \
  -e DATAHUB_GMS_PORT=8080 \
  -e DATAHUB_EXECUTOR_EXTRA_PACKAGES="observe-models==1.0.0 custom-plugin>=2.0" \
  -e DATAHUB_EXECUTOR_EXTRA_PACKAGES_STRICT=true \
  datahub-executor:latest
```

**Notes:**

- Runtime installs use whatever `UV_EXTRA_INDEX_URL` / `PIP_EXTRA_INDEX_URL` is baked into the image at build time (including Cloudsmith if configured).
- This feature is **not available** on `locked` variants (`locked-wolfi`, `locked-alpine`) since they block network access to PyPI for security.
- For production deployments, prefer building custom images with bundled venvs over runtime installation for faster startup and reproducibility.

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

## Slim Plugin Variants

Some plugins have `-slim` variants that exclude PySpark dependencies for smaller image size:

| Plugin | Regular              | Slim                      | Difference                     |
| ------ | -------------------- | ------------------------- | ------------------------------ |
| `s3`   | `acryl-datahub[s3]`  | `acryl-datahub[s3-slim]`  | No PySpark/data-lake-profiling |
| `gcs`  | `acryl-datahub[gcs]` | `acryl-datahub[gcs-slim]` | No PySpark/data-lake-profiling |
| `abs`  | `acryl-datahub[abs]` | `acryl-datahub[abs-slim]` | No PySpark/data-lake-profiling |

When `BUNDLED_VENV_SLIM_MODE=true` (default in bundled builds), plugins with slim variants automatically use the slim version.

## Provenance Tracking

Bundled images include provenance metadata at `/opt/datahub/venvs/metadata.json`:

```json
{
  "bundled_plugins": ["s3", "bigquery", "looker"],
  "cli_version": "1.3.1.4",
  "slim_mode": "true",
  "github_run_id": "12345678"
}
```

To inspect an image's bundled configuration:

```bash
docker run --rm datahub-executor:<tag>-locked-wolfi \
  cat /opt/datahub/venvs/metadata.json | jq
```

## Troubleshooting / FAQ

- Why does the build complain that `build_bundled_venvs_unified.py` is missing?
  - Ensure you are extending the slim image: it contains the builder scripts at `/usr/local/bin`. If you start FROM the full image, the required tools may be missing or not intended for reuse.
  - To verify inside a built image:
    - `docker run --rm -it <your-image> bash -lc 'ls -l /usr/local/bin/build_bundled_venvs_unified.*'`
- Is this related to test vs actual build?
  - A unit/CI test may execute scripts on the host filesystem, where `/usr/local/bin/build_bundled_venvs_unified.py` does not exist. The Docker image contains the scripts. For tests that need to execute the builder, run inside the image/container or reference the repo path version `docker/snippets/ingestion/build_bundled_venvs_unified.py`.
- Do I need to set `PIP_INDEX_URL`/`PIP_EXTRA_INDEX_URL`?
  - No for standard internet-connected builds (defaults to public PyPI). Yes only for air-gapped/mirrored environments, and they should be provided at build time so dependencies are fetched during the image build.
