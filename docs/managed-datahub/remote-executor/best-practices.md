---
title: Remote Executor best practices
description: Sizing, resource allocation, storage, and virtual-environment tuning for Remote Executor deployments.
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Remote Executor best practices

<FeatureAvailability saasOnly />

Operational guidance for running Remote Executors reliably — how to size CPU, memory, and storage for your ingestion
workload, and how the executor uses ephemeral disk so you can keep it from filling up. This is a living document; sections
are added as recurring operational questions come up.

## Resource allocation

An executor runs multiple ingestion and observe tasks in parallel. CPU, memory, and disk each
scale differently with that concurrency, so size them independently rather than picking a single "instance size".

### CPU and parallelism

Core count should track **how many ingestions you want to run at once**. Each concurrent task is largely single-threaded
from the executor's point of view, so a reasonable starting point is roughly one core per parallel ingestion, plus one for
the executor's own coordination.

The number of parallel tasks is capped by environment variables:

- **`DATAHUB_EXECUTOR_INGESTION_MAX_WORKERS`** (default `4`) — maximum concurrent Ingestion tasks.
- **`DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS`** (default `10`) — maximum concurrent Observe monitor tasks.

In the DataHub-provided Helm chart these map to `datahub.executor_ingestions_workers` / `datahub.executor_monitors_workers`.
If you raise the worker count, raise cores to match — otherwise tasks contend for CPU and every run gets slower.

The general recommendation would be to assign cores to the below formula:

```
1 + n + k
```

Where:

- `n` - number of expected concurrent ingestion runs handled by the executor
- `k` - 1 if Observe functionalities are enabled for resources ingested by the executor, else 0

### Memory

Memory is the hardest resource to size, because per-ingestion usage varies enormously — anywhere from **~100 MB** for a
small source to **4–6 GB** (occasionally more, in rare cases) for a large one. It scales with both the **type** of source and
the **amount** of metadata pulled in a single run.

As a rule of thumb, data-warehouse ingestions — **Snowflake, BigQuery, Redshift** — are the heavy hitters, routinely peaking
around **4–6 GB** each when profiling or extracting large schemas and lineage.

Because tasks run concurrently, size memory for the **expected peak of all runs happening at the same time**, not the
average of a single run: `n` warehouse ingestions scheduled together can momentarily need roughly `n × 6 GB`.

Because of this, **Stagger large ingestions.** Schedule big warehouse runs so they don't overlap. This is the simplest
and most effective lever — two 6 GB runs an hour apart need 6 GB of headroom; the same two at once need 12 GB.

Note that the executor itself consumes some memory, as does each running Observe task.

### Storage

Two things consume the executor's ephemeral disk (under `/tmp/datahub/`) during a run, and they are reclaimed differently:

- **Dynamic virtual environments** under `/tmp/datahub/ingest/<execution-id>/`, built per run for non-bundled CLI versions
  and connectors. Each is deleted when its run ends, so venv disk is released continuously as runs finish. The shared `uv`
  cache that keeps this bounded is covered in [the uv cache](#ingestion-virtual-environments-and-the-uv-cache) below.
- **Per-execution logs** under `/tmp/datahub/logs/`. Unlike venvs, logs are **kept after the run** so you can inspect them
  directly (logs visible via UI are stored in DataHub and are truncated)

By default nothing prunes the logs. Enable the in-process **log garbage collector** with
**`DATAHUB_EXECUTOR_LOG_GC_ENABLED=true`**: it scans the log directory on a timer and deletes per-execution directories
older than the retention window, with a size cap as a safety net. Tunables:

| Variable                                              | Default             | Purpose                                                                                 |
| ----------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------- |
| **`DATAHUB_EXECUTOR_LOG_GC_ENABLED`**                 | `false`             | Master switch for the log GC.                                                           |
| **`DATAHUB_EXECUTOR_LOG_DIR`**                        | `/tmp/datahub/logs` | Directory scanned.                                                                      |
| **`DATAHUB_EXECUTOR_LOG_GC_RETENTION_DAYS`**          | `14`                | Age threshold for deletion.                                                             |
| **`DATAHUB_EXECUTOR_LOG_GC_MAX_DIR_SIZE_MB`**         | `10000`             | Size-cap safety net; oldest logs are removed first when exceeded. `0` disables the cap. |
| **`DATAHUB_EXECUTOR_LOG_GC_INTERVAL_SECONDS`**        | `3600`              | How often the GC runs.                                                                  |
| **`DATAHUB_EXECUTOR_LOG_GC_IN_FLIGHT_GRACE_SECONDS`** | `3600`              | Protects logs of recently or still-running executions from deletion.                    |

Size ephemeral storage — Fargate
[`ephemeral_storage`](../operator-guide/setting-up-remote-ingestion-executor.md#deploy-on-amazon-ecs), or the node /
`emptyDir` backing `/tmp` on Kubernetes — for the largest single venv, plus the `uv` cache, plus your retained-log budget.

## Ingestion virtual environments and the uv cache

Runs that target a **non-[bundled](/docs/docker/bundled-ingestion-venvs.md)** CLI version or connector build a **dynamic
virtual environment** per execution under `/tmp/datahub/ingest/<execution-id>/`, removed when the run ends. To stop repeated
installs from filling ephemeral storage, package installs go through the [`uv`](https://docs.astral.sh/uv/) cache: each
package is unpacked **once** into `UV_CACHE_DIR` (by default: `$HOME/.cache/uv`) and shared across venvs,
so many runs that share a dependency pay for its bytes roughly once rather than once per run.

The DataHub-provided Helm chart, Terraform module, and CloudFormation template set **`UV_LINK_MODE=hardlink`** by default,
which links files from the cache instead of copying them — the most space-efficient way to share it. Other modes (`copy`,
`clone`, `symlink`) trade space for portability. See
[Dynamic venvs and the uv cache](/docs/docker/bundled-ingestion-venvs.md#dynamic-venvs-and-the-uv-cache-non-bundled-runs)
for the full mechanism and the `UV_LINK_MODE` options.

For the cache to actually save space, it has to sit on the same filesystem as the venvs it feeds:

:::caution Keep the uv cache and `/tmp` on the same filesystem
Hardlinks (and `clone` reflinks) only work **within one filesystem**. If you mount a separate volume or an `emptyDir` at
`/tmp` — for a bigger ingestion disk, or to satisfy a read-only root — but leave the cache on the container root, uv cannot
link and **falls back to copying**, so every venv consumes the full size of its dependencies. Put the cache on the same
volume as `/tmp/datahub/ingest` with **`UV_CACHE_DIR`**, or keep both on the container root.
:::

## Read-only root filesystem

The default cache path (`$HOME/.cache/uv`, i.e. `/home/datahub/.cache/uv`) lives on the container **root**. If you set
`readOnlyRootFilesystem: true` (Kubernetes) or `readonly_root_filesystem = true` (ECS), uv cannot write its cache there and
**dynamic venv builds fail outright** — non-bundled runs will not execute at all. This is not about losing the storage
savings; the runs stop working.

Point **`UV_CACHE_DIR`** at a writable volume. Mounting a single `emptyDir` at `/tmp` and placing the cache under it
satisfies both the writability and the same-filesystem requirement at once:

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
    value: /tmp/uv-cache # same emptyDir as /tmp/datahub/ingest → links work
```

Deployments that pin **every** source to the bundled CLI version don't build dynamic venvs and are unaffected.
