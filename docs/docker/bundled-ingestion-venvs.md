---
title: Bundled ingestion virtual environments
description: Pre-built ingestion venvs under /opt/datahub/venvs, extending datahub-actions or datahub-executor images at build time.
---

# Bundled ingestion virtual environments

UI and scheduled ingestion run `datahub ingest` in **pre-built venvs** under **`DATAHUB_BUNDLED_VENV_PATH`** (default **`/opt/datahub/venvs`**). Each connector uses a **`{plugin}-bundled`** path; optional **named groups** install once into **`{label}-venv`** and symlink member plugins there.

The executor uses those installs when the run targets the **bundled CLI version** (aligned with **`BUNDLED_CLI_VERSION`**). Connector installs are **baked at image build time**—runtime env vars alone do not add new venvs.

## Core (`datahub-actions`) vs Cloud (`datahub-executor`)

| Offering          | Image (typical)                                                                   | Role                                  |
| ----------------- | --------------------------------------------------------------------------------- | ------------------------------------- |
| **DataHub Core**  | [`acryldata/datahub-actions`](https://hub.docker.com/r/acryldata/datahub-actions) | Executor for UI / scheduled ingestion |
| **DataHub Cloud** | **`datahub-executor`** (your registry)                                            | Remote Executor                       |

Same layout and env contract; only the image name changes. **Full** and **slim** **`datahub-actions`** tags ship **`/opt/datahub/bundled-venv-build/`** (builder scripts + **`constraints.txt`**) so you can extend **from the base image** without cloning the repo. Remote Executor images may ship the same path; if not, see [No builder directory in the image](#no-builder-directory-in-the-image).

## Variables

Used by **`build_bundled_venvs_unified.sh`** / **`.py`**. Published **`datahub-actions`** images also set matching **`ENV`** values (path, plugin lists, **`BUNDLED_CLI_VERSION`**, **`BUNDLED_VENV_SLIM_MODE`**) so **`FROM`** inherits them.

| Variable                            | Meaning                                                                                                                                                                           |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`BUNDLED_VENV_PLUGINS`**          | Every plugin that gets a **`{plugin}-bundled`** path (comma-separated).                                                                                                           |
| **`BUNDLED_VENV_PLUGINS_<suffix>`** | Plugins sharing one install → **`{suffix_lower}-venv`** (e.g. **`COMMON`** → **`common-venv`**).                                                                                  |
| **`BUNDLED_CLI_VERSION`**           | Required by the shell wrapper. With **`/metadata-ingestion`** in the image, installs are editable; still must be set. PyPI-only builds need the real **`acryl-datahub`** version. |
| **`BUNDLED_VENV_SLIM_MODE`**        | **`true`** uses **`-slim`** extras where applicable and checks PySpark is absent in slim builds.                                                                                  |
| **`DATAHUB_BUNDLED_VENV_PATH`**     | Root for venvs (default **`/opt/datahub/venvs`**).                                                                                                                                |

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

**Locked** (**`*-locked`**) images remove **`uv`**/**`pip`**—do not use them as the base for this flow.

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

## Rebuild from this repository

Maintainers: **`docker/datahub-actions/Dockerfile`** + **`--build-arg`** (see Dockerfile and snippet README)—requires a repo checkout.

## Related documentation

- [Ingestion Executor](/docs/actions/actions/executor.md)
- [Docker development](/docs/docker/development.md)
