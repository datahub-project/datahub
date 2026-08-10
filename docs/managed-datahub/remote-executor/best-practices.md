---
title: Remote Executor best practices
description: Storage and virtual-environment tuning for Remote Executor deployments
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Remote Executor best practices

<FeatureAvailability saasOnly />

## Ingestion virtual environments and disk usage

Runs that target a **non-[bundled](/docs/docker/bundled-ingestion-venvs.md)** CLI version or connector build a **dynamic virtual
environment** per execution under `/tmp/datahub/ingest/<execution-id>/`, which is removed when the run ends. To keep these from
repeatedly filling ephemeral storage, installs go through the [`uv`](https://docs.astral.sh/uv/) cache: each package is unpacked
**once** into `$HOME/.cache/uv`, and every venv **hardlinks** its files from that cache instead of copying them — so many runs that
share a dependency cost its bytes once, not once per run. This is the default (`UV_LINK_MODE=hardlink`) in the DataHub-provided Helm
chart, Terraform module, and CloudFormation template. See
[Dynamic venvs and the uv cache](/docs/docker/bundled-ingestion-venvs.md#dynamic-venvs-and-the-uv-cache-non-bundled-runs) for the
full mechanism and the `UV_LINK_MODE` options.

Two deployment layouts silently break hardlinking and bring the storage growth back:

:::caution Keep the uv cache and `/tmp` on the same filesystem
Hardlinks only work **within one filesystem**. If you mount a separate volume or an `emptyDir` at `/tmp` (for a bigger ingestion
disk, or to satisfy a read-only root) but leave the cache on the container root, uv cannot hardlink and **falls back to copying** —
every venv then consumes the full size of its dependencies. Put the cache on the same volume as `/tmp/datahub/ingest` with
**`UV_CACHE_DIR`**, or keep both on the container root.
:::

:::caution Read-only root filesystem
The default cache path (`$HOME/.cache/uv`, i.e. `/home/datahub/.cache/uv`) lives on the container **root**. If you set
`readOnlyRootFilesystem: true` (Kubernetes) or `readonly_root_filesystem = true` (ECS), uv cannot write its cache there and venv
builds fail. Point **`UV_CACHE_DIR`** at a writable volume that is the **same** filesystem as `/tmp/datahub/ingest` — mounting one
`emptyDir` at `/tmp` and placing the cache under it satisfies both requirements at once:

```yaml
# Kubernetes (datahub-executor-worker values.yaml)
extraVolumes:
  - name: ingest-tmp
    emptyDir: {}
extraVolumeMounts:
  - name: ingest-tmp
    mountPath: /tmp
extraEnvs:
  - name: UV_CACHE_DIR
    value: /tmp/uv-cache # same emptyDir as /tmp/datahub/ingest → hardlinks work
```
:::

Even with hardlinking, size ephemeral storage — Fargate
[`ephemeral_storage`](../operator-guide/setting-up-remote-ingestion-executor.md#deploy-on-amazon-ecs), or the node / `emptyDir`
backing `/tmp` on Kubernetes — for the largest single venv plus the cache. Hardlinking removes the per-run multiplier across
concurrent runs, not the one-time floor.
