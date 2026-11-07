---
title: Upgrading DataHub
description: "How to move a DataHub deployment to a newer version: what to check first, how to run the upgrade, and how to verify it afterwards."
---

# Upgrading DataHub

This page is the order of operations for moving an existing deployment to a newer version. The
version-specific details live in [Breaking Changes by Version](breaking-changes.md); this page is
about when to read them and what to do around them.

## Before you upgrade

### 1. Take a backup

Metadata lives in two places and each needs its own backup. [Backup & Restore](backup-datahub.md)
covers both: dumping the `metadata_aspect_v2` table, and snapshotting the Elasticsearch or
OpenSearch indices.

For a local quickstart instance, `datahub docker quickstart --backup` is enough. See
[Back up DataHub](../quickstart.md#back-up-datahub).

:::caution

An upgrade can reindex or migrate data. Some steps are not reversible, and DataHub has no
automated rollback. A restorable backup is what makes a failed upgrade recoverable.

:::

### 2. Read the breaking changes for every version you are passing through

Not only the version you are moving to. Going from 1.5.0 to 1.8.0 means reading 1.6.0, 1.7.0 and
1.8.0 in [Breaking Changes by Version](breaking-changes.md). Each entry states whether any action
is required.

### 3. Check the version requirements and expected downtime

Each release lists the CLI and Helm chart versions it expects, and a **Potential Downtime**
section describing reindexing or migrations that run on startup. Plan the window around the
largest one you are passing through, not the last.

Cassandra is not a supported upgrade path, and ZDU is not supported on it. An install that already
has Search V2 indices keeps serving and writing V2 after the upgrade. To copy existing metadata into
Search V3, re-ingest after the upgrade. Do not enable ZDU on Cassandra. See
[#21030](https://github.com/datahub-project/datahub/pull/21030) and
[Moving search to V3](#moving-search-to-v3).

## Moving search to V3

Search V3 is the default entity index. A fresh install reads and writes only V3, and this section
does not apply to it. An install that already has Search V2 entity indices keeps serving V2 after
the upgrade, and keeps writing V2, until each of its V3 indices is complete.

At startup, GMS and the MAE and MCE consumers check every V2 entity index. For each one that exists,
they check its paired V3 index for the `complete` mark that catch-up writes after the fill. While any
paired V3 index is missing or not marked complete, that process reads V2, writes V2, and keeps V3
keyword reads and hybrid reads off. V3 writes continue, so the V3 indices keep filling. Setting the
V3 read flags by hand does not change this while a paired index is incomplete.

To move reads to V3:

1. On Ebean, run the upgrade with incremental reindex on (`ZDU_STAGE_20=true`, or
   `ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED=true`). System update creates the V3
   indices, and the non-blocking job fills them from primary storage. Catch-up marks each one
   complete when it finishes. Cassandra cannot run this fill, so leave ZDU off there.
2. Restart GMS, the MAE consumer, and the MCE consumer together, after every V3 index is marked
   complete. That restart moves reads to V3 and stops V2 writes. Replace all three in the same
   window: a process still on V2 reads misses documents that are written only to V3 after the
   restart.

Without incremental reindex the mark never arrives, so search stays on V2. Search is not down during
the wait, and the restart in step 2 is the only search cutover.

Cassandra cannot run the fill. An install that has V2 indices stays on V2 there. A fresh Cassandra
install has no V2 index, so this hold does not apply. See the Cassandra note in
[Potential Downtime](#3-check-the-version-requirements-and-expected-downtime) above.

If DataHub cannot list the V2 entity indices at startup, the process stops rather than treating the
install as new.

## Run the upgrade

### Kubernetes

Bump the chart version, `global.datahub.version`, or both, then upgrade:

```bash
helm repo update datahub
helm upgrade --install datahub datahub/datahub --values values.yaml
```

The chart runs a **system-update** Job that applies schema changes and reindexes when needed.
Components do not wait for it, and a blocking step can hold the rollout until it finishes. See
[System update and upgrades](../deploy/kubernetes.md#system-update-and-upgrades) for what the Job
does and how scale-down behaves.

If your pipeline renders the chart and applies the YAML itself, with Spinnaker, Kustomize or
`kubectl`, follow [Upgrading with rendered manifests](../deploy/rendered-manifest-upgrade.md)
instead. Applying a whole render in one step starts the new pods and both Jobs together, which is
not a zero-downtime upgrade.

### Docker quickstart

```bash
datahub docker quickstart
```

This pulls the newest images and recreates the containers. Upgrading from a CLI older than 1.2
needs extra steps, because the compose file changed; see
[Managing Your Local Instance](../quickstart.md#managing-your-local-instance).

## After the upgrade

1. Check that GMS is healthy and the UI loads.
2. Search a known entity and open its lineage. If results are missing or stale, the indices may
   not have finished rebuilding — give the system-update Job time to complete before acting.
3. If search or graph data is still wrong once it has finished, rebuild from the source of truth
   with [Restore Indices](restore-indices.md).
4. Run an ingestion job and confirm it completes.

## If the upgrade fails

There is no automated rollback. Reverting means redeploying the previous version and, if the
upgrade had already migrated or reindexed data, restoring from the backup taken beforehand. The
restore procedures are in [Backup & Restore](backup-datahub.md).
