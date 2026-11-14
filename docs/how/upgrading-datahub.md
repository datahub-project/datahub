
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
