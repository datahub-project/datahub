---
title: Ingestion executor security and hardening
description: Trust boundaries for UI-driven ingestion, locked datahub-actions images, and controlling Python package indexes for the ingestion executor.
---

# Ingestion executor security and hardening

The **ingestion executor** runs recipes submitted from DataHub (for example via managed ingestion sources). Treat it as infrastructure that executes **user-influenced configuration**: anyone who can **create or edit** those sources controls recipe fields the executor processes. Combine that with **least privilege** for platform privileges (for example **Manage Metadata Ingestion**, secrets access, and executor-related tokens) so only trusted operators can define scheduled or ad hoc runs.

The executor Action itself requires a privileged token for secret resolution; see [Ingestion Executor](/docs/actions/actions/executor.md) for prerequisites.

## Locked (`*-locked`) images

**Locked** variants of **`datahub-actions`** (tags such as **`*-locked`**) are intended for deployments that rely on **pre-built bundled virtual environments** under **`DATAHUB_BUNDLED_VENV_PATH`** (default **`/opt/datahub/venvs`**) instead of installing connectors at container runtime. In those images, **`pip`** and **`uv`** are removed from the runtime image, and default package index URLs are set to unusable localhost endpoints as defense in depth—see the **`final-locked`** stage in **`docker/datahub-actions/Dockerfile`** in the repository.

**Tradeoff:** You cannot extend a locked image at runtime with arbitrary **`pip install`**. Add connectors by **rebuilding** the image with the bundled venv builder and variables such as **`BUNDLED_VENV_PLUGINS`** and **`BUNDLED_CLI_VERSION`**. Do not use locked images as the base for “append plugins via Dockerfile **RUN** pip” flows that expect a package manager in the final layer; see [Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md).

Remote Executor (**`datahub-executor`**) images follow the same bundled-venv contract where applicable; deployment details: [Configuring Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md).

## Package index control (private mirror / PyPI proxy)

Even when connectors are resolved from PyPI, **controlling which indexes the environment can use** reduces supply-chain risk: route installs through an **internal mirror** (for example Artifactory, Nexus, or DevPI), allow only approved packages, and restrict **outbound egress** so executors cannot reach arbitrary third-party index URLs.

When **building** custom images from the DataHub Dockerfiles, you can steer installs at build time using mirror-related build arguments and environment variables set on the Python base image (for example **`PIP_MIRROR_URL`**, **`PIP_EXTRA_INDEX_URL`**, and **`UV_INDEX_URL`** in **`docker/datahub-actions/Dockerfile`**). Align runtime or orchestration-level networking with the same policy so executor workloads cannot bypass your mirror.

Non-locked images may still expose package managers at runtime; **locked** images aim to avoid relying on runtime installers altogether. Operational controls (mirror, egress, RBAC) remain **defense in depth** and should not replace reviewing trust boundaries above.

## Related documentation

- [Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md)
- [Ingestion Executor](/docs/actions/actions/executor.md)
- [Configuring Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md)
