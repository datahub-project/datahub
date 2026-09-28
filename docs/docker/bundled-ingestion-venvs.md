---
title: Bundled ingestion virtual environments
description: Pre-built ingestion venvs under /opt/datahub/venvs, extending datahub-actions or datahub-executor images at build time.
---

# Bundled ingestion virtual environments

UI and scheduled ingestion run `datahub ingest` in **pre-built venvs** under **`DATAHUB_BUNDLED_VENV_PATH`** (default **`/opt/datahub/venvs`**). Each connector uses a **`{plugin}-bundled`** path; optional **named groups** install once into **`{label}-venv`** and symlink member plugins there.

The executor uses those installs when the run targets the **bundled CLI version** (aligned with **`BUNDLED_CLI_VERSION`**). Connector installs are **baked at image build time**—runtime env vars alone do not add new venvs.

## Using a bundled venv

A run uses a bundled venv only when the source's **CLI version** is set to **`bundled`** — the `version` execution argument (defaults to `latest`). In the DataHub UI, set it on the ingestion source under **Advanced → CLI Version**:

- **`bundled`** → run from the pre-built **`/opt/datahub/venvs/{plugin}-bundled`** (read-only, **no** runtime install, no ephemeral-storage growth).
- empty (default) or **`latest`** or a specific `acryl-datahub` version → build a [dynamic venv](#dynamic-venvs-and-the-uv-cache-non-bundled-runs) at runtime.

If the requested plugin isn't bundled in the image, the run falls back to a dynamic venv. If the image does not have access to a PyPI repository or it is **locked** (it has `uv` and `pip` binaries deliberately removed), the run will fail.

## Core (`datahub-actions`) vs Cloud (`datahub-executor`)

| Offering          | Image (typical)                                                                   | Role                                  |
| ----------------- | --------------------------------------------------------------------------------- | ------------------------------------- |
| **DataHub Core**  | [`acryldata/datahub-actions`](https://hub.docker.com/r/acryldata/datahub-actions) | Executor for UI / scheduled ingestion |
| **DataHub Cloud** | **`datahub-executor`** (your registry)                                            | Remote Executor                       |

Same layout and env contract; only the image name changes. **Full** and **slim** **`datahub-actions`** tags ship **`/opt/datahub/bundled-venv-build/`** (builder scripts + **`constraints.txt`**) so you can extend **from the base image** without cloning the repo. Remote Executor images may ship the same path; if not, see [No builder directory in the image](#no-builder-directory-in-the-image).

## Variables

Used by **`build_bundled_venvs_unified.sh`** / **`.py`**. Published **`datahub-actions`** images also set matching **`ENV`** values (path, plugin lists, **`BUNDLED_CLI_VERSION`**, **`BUNDLED_VENV_SLIM_MODE`**) so **`FROM`** inherits them.

| Variable                            | Meaning                                                                                                                                                                                        |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`BUNDLED_VENV_PLUGINS`**          | Every plugin that gets a **`{plugin}-bundled`** path (comma-separated).                                                                                                                        |
| **`BUNDLED_VENV_PLUGINS_<suffix>`** | Plugins sharing one install → **`{suffix_lower}-venv`** (e.g. **`COMMON`** → **`common-venv`**).                                                                                               |
| **`BUNDLED_CLI_VERSION`**           | PyPI pin for **`acryl-datahub`**. Published images set this as **`ENV`** so extenders inherit it. If unset, the shell wrapper falls back to the installed **`acryl-datahub`** package version. |
| **`BUNDLED_VENV_SLIM_MODE`**        | **`true`** uses **`-slim`** extras where applicable and checks PySpark is absent in slim builds.                                                                                               |
| **`DATAHUB_BUNDLED_VENV_PATH`**     | Root for venvs (default **`/opt/datahub/venvs`**).                                                                                                                                             |

Each plugin appears **once** across groups or as a singleton; group lists ⊆ **`BUNDLED_VENV_PLUGINS`**. More detail lives in the repo at **`docker/snippets/ingestion/README.md`** (bundled venv builder configuration).

## Extend a published image

Tags look like **`v1.6.0-slim`** (slim) or **`v1.6.0`** / **`v1.6.0-full`** (full—names vary by registry). Append plugins by overriding **`ENV`**, then **`RUN`** the builder:

```dockerfile
FROM acryldata/datahub-actions:v1.6.0-slim

USER root

ENV BUNDLED_VENV_PLUGINS="${BUNDLED_VENV_PLUGINS},mysql,snowflake"
ENV BUNDLED_VENV_PLUGINS_COMMON=${BUNDLED_VENV_PLUGINS}

RUN /opt/datahub/bundled-venv-build/build_bundled_venvs_unified.sh

USER datahub
```

Docker substitutes **`${BUNDLED_VENV_PLUGINS}`** from the parent image so you need not repeat the base list. **`docker build`** needs network for **`uv`**/**`pip`**.

Match **`--platform`** (and the base image variant) to the CPU architecture of the nodes that run the executor — bundled venvs contain arch-specific wheels. Use a **single-platform** build when the fleet is uniform, or a **multi-arch** Buildx image when nodes are mixed. Details: [CPU architecture](/docs/managed-datahub/remote-executor/bundling-additional-connectors.md#cpu-architecture).

**Locked** (**`*-locked`**) images remove **`uv`**/**`pip`**—do not use them as the base for this flow.

**Security hardening:** Trust boundaries for executor workloads, why locked images remove runtime package managers, and how to steer installs through an internal PyPI mirror are covered in [Ingestion executor security and hardening](/docs/docker/ingestion-executor-security.md).

### Remote Executor (`datahub-executor`)

Same **`ENV`** + **`RUN`** pattern if your image includes **`/opt/datahub/bundled-venv-build/`**. Otherwise see below or ask DataHub Cloud for a custom image. Deploy help: [Configuring Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md).

## No builder directory in the image

Copy these files into one directory (e.g. **`/opt/datahub/bundled-venv-build/`**), **`chmod +x`** the **`.sh`**, ensure **`constraints.txt`** exists under **`DATAHUB_BUNDLED_VENV_PATH`**, set the same env vars, run **`build_bundled_venvs_unified.sh`**. Pin **`raw.githubusercontent.com`** to a **commit SHA**. Files: **`build_bundled_venvs_unified.py`**, **`build_bundled_venvs_unified.sh`**, **`bundled_venv_config.py`**, **`constraints.txt`** under **`docker/snippets/ingestion/`**.

## Advanced: extra venv groups

Default is one **common** group via **`BUNDLED_VENV_PLUGINS_COMMON`** for a smaller image. Add **`BUNDLED_VENV_PLUGINS_<suffix>`** only when connectors **cannot share one env** (conflicting transitive deps). Example:

```dockerfile
ENV BUNDLED_VENV_PLUGINS=s3,demo-data,file,mysql,oracle
ENV BUNDLED_VENV_PLUGINS_COMMON=s3,demo-data,file,mysql
ENV BUNDLED_VENV_PLUGINS_ORACLE=oracle
```

If everything resolves in one venv, avoid extra groups.

## Dynamic venvs and the uv cache (non-bundled runs)

When a run targets a CLI version or connector that is **not** bundled into the image, the executor builds a **dynamic venv** at runtime under **`/tmp/datahub/ingest/<execution-id>/`** and removes it when the run finishes. To keep repeated runs from filling disk, installs go through the [`uv`](https://docs.astral.sh/uv/) package cache: each package is unpacked **once** into a content-addressed cache (default **`$HOME/.cache/uv`**), and every venv **links** its files from that cache instead of copying them. This applies to both DataHub Core (**`datahub-actions`**) and DataHub Cloud (**`datahub-executor`**).

The link method is controlled by **`UV_LINK_MODE`**:

| Mode       | Behavior                                                                                                                                                                                                                                              |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hardlink` | Same-inode hardlinks from the cache. Smallest, most predictable footprint; **requires the cache and the venv directory to be on the same filesystem.**                                                                                                |
| `clone`    | Copy-on-write reflinks on reflink-capable filesystems (XFS-reflink, btrfs, ZFS, APFS); **silently falls back to a full copy** when the active filesystem doesn't support reflinks (typically the overlay filesystems used by Kubernetes and Fargate). |
| `copy`     | Full byte copy per venv. Always works, largest footprint.                                                                                                                                                                                             |
| `symlink`  | Symlinks into the cache. Breaks if the cache is pruned or lives on a different mount at runtime.                                                                                                                                                      |

uv's default link mode falls back to copying on overlay filesystems, so many runs each duplicate their dependencies. Setting **`UV_LINK_MODE=hardlink`** links them instead — N venvs that share a dependency cost its bytes once, not N times. The DataHub-provided Remote Executor Helm chart, Terraform module, and CloudFormation template default to `hardlink`.

Two constraints follow from how hardlinks work:

- **Same filesystem.** Hardlinks cannot cross filesystems. If `$HOME/.cache/uv` and `/tmp/datahub/ingest` are on different mounts (e.g. an `emptyDir` mounted at `/tmp` but the cache left on the container root), uv cannot link and falls back to copying — the run still succeeds, but the dedup savings are lost. Keep both on one volume, or move the cache with **`UV_CACHE_DIR`**.
- **Read-only root.** The default cache path is on the container root filesystem. With a read-only root, point **`UV_CACHE_DIR`** at a writable volume — ideally the same one backing `/tmp/datahub/ingest`, so hardlinking keeps working.

Repeated runs that use the same package artifacts reuse the cache, but new package versions, platforms, and build artifacts can continue to grow it. Monitor cache usage on long-lived executors and reclaim space with `uv cache prune` when needed.

## Rebuild from this repository

Maintainers: **`docker/datahub-actions/Dockerfile`** + **`--build-arg`** (see Dockerfile and snippet README)—requires a repo checkout.

## Related documentation

- [Ingestion executor security and hardening](/docs/docker/ingestion-executor-security.md)
- [Ingestion Executor](/docs/actions/actions/executor.md)
- [Docker development](/docs/docker/development.md)
