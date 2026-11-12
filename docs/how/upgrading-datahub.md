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
   complete when it finishes, which can be after system update has already exited. Cassandra cannot
   run this fill, so leave ZDU off there.
2. Restart GMS, the MAE consumer, and the MCE consumer together, as soon as every V3 index is marked
   complete. The hold is decided once, when a process starts. A process that starts in between — a
   crash, an eviction, a scale-out, a node drain — sees every index complete, drops the hold, and
   stops writing V2, while processes that are still running keep reading V2. Their search, and the
   policies they load, then miss whatever the new process indexed. Do not roll one of the three
   early. An index is complete when system update logs `migration marked complete` for it, or when
   `GET <index>/_mapping` shows `_meta.migration` of `complete`.

That restart moves reads to V3 and stops V2 writes. It also marks each existing V2 entity index
retired (`_meta.v3Cutover` of `retired`). A later start sees that mark and keeps reading V3,
including after a V3 reindex drops `_meta.migration`. If a retired install cannot read a V3 index at
startup, GMS and the consumers stop rather than serving the V2 copy. An index marked `incomplete`
is treated the same way, including when it already has documents and when its document count cannot
be read. Returning at the first document would serve a partial search and a partial policy set until
catch-up finishes. System update and LoadIndices serve no reads, so they continue and BuildIndices
can recreate the index. The incremental fill stamps `incomplete` when it creates the index, and a
legacy create does the same when the paired V2 index is already retired or cannot be listed. A
failed stamp fails that create. Readers stay down until catch-up marks the index `complete`.
Recreating it needs incremental reindex on (`ZDU_STAGE_20=true`, or
`ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED=true`).

### Installs that already read Search V3

An install that turned V2 off and V3 keyword read on before this upgrade, including a Helm install
set to search mode `v3`, still has its old V2 indices. Those indices stopped receiving writes when
V2 was turned off, and the V3 indices were never stamped `complete`. Startup cannot tell that apart
from a first migration, so it would serve the stale V2 copy: entities created since the switch are
missing, updates and deletes are served as they were at the switch, and a policy created since is
not loaded.

Before upgrading, do one of the following:

- Set `ELASTICSEARCH_ENTITY_INDEX_V3_CUTOVER_ACKNOWLEDGED=true` on GMS, both consumers, and system
  update for the upgrade start. This is a one-start statement from the operator. A chart must not
  derive it from the search mode. Startup stamps each V2 index `_meta.v3Cutover` of `retired` and
  keeps reading V3. Later starts trust the stamp even if the flag stays set, and do not compare
  document counts again. It does not skip a first migration. It stamps V2 retired when every paired
  V3 index exists, is not marked `incomplete`, and at least one holds a document of its entity type.
  A type whose V2 index has no documents is unused, so an empty V3 count for it does not block the
  cutover. A count below the paired V2 index still qualifies when that type has at least one V3
  document. A V3 index that is missing, incomplete, or empty while its V2 index has documents stays
  on V2 while every pair is in that state. When some V3 index has documents of its entity type and
  another is missing, incomplete, empty of a type its V2 index still holds, or cannot be counted,
  GMS and the consumers stop instead of serving V2. A V3 index whose listing or mapping cannot be
  read also stops GMS and the consumers instead of serving V2, even when every other V3 index has
  documents. System update, restore, and LoadIndices keep going so a missing index can be recreated.
  Cleanup keeps going so it can still tear indices down.
- Or stamp `_meta.v3Cutover` to `retired` on each V2 entity index, or delete those indices, before
  upgrading.

Without one of those, search and newly created policies follow the stale V2 copy until a fill
rebuilds V3. On Cassandra that fill never runs, so the copy stays stale until the V2 indices are
removed or stamped.

Without incremental reindex the mark never arrives, so search stays on V2. Search is not down during
the wait, and the restart in step 2 is the only search cutover.

An install whose first upgrade ran with incremental reindex off has V3 indices created on the legacy
path, with documents and no migration mark. Turning incremental reindex on later rebuilds each of
those indices from primary storage while its paired V2 index still exists, and catch-up then marks
it complete. A V3 index with no paired V2 index is a fresh install and is left as it is.

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
