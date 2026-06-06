---
description: "Configure database retention policies in DataHub to control how long historical metadata aspects and timeseries data are retained."
---

# Configuring Database Retention

## Goal

DataHub stores different versions of [metadata aspects](https://docs.datahub.com/docs/what/aspect) as they are ingested
using a database (or key-value store). These multiple versions allow us to look at an aspect's historical changes and
rollback to a previous version if incorrect metadata is ingested. However, every stored version takes additional storage
space, while possibly bringing less value to the system. We need to be able to impose a **retention** policy on these
records to keep the size of the DB in check.

Goal of the retention system is to be able to **configure and enforce retention policies** on documents at each of these
various levels:

- global
- entity-level
- aspect-level

## What type of retention policies are supported?

We support 3 types of retention policies for aspects:

|    Policy     |             Versions Kept             |
| :-----------: | :-----------------------------------: |
|  Indefinite   |             All versions              |
| Version-based |          Latest _N_ versions          |
|  Time-based   | Versions ingested in last _N_ seconds |

**Note:** The latest version (version 0) is never deleted. This ensures core functionality of DataHub is not impacted while applying retention.

## When is the retention policy applied?

As of now, retention policies are applied in two places:

1. **System-update (datahub-upgrade)**: The default retention policies are ingested when you run the **non-blocking** system-update (`IngestRetentionPolicies` step). The default YAML is bundled in the datahub-upgrade module (`boot/retention.yaml`). On each system-update run, policies are compared to the stored config at each exact `(entity, aspect)` key; when the desired config differs and the stored policy is system-managed (or you opt in to overwrite), the policy is updated. When policies change, a batch apply trims historical aspect versions (enabled by default via `ENTITY_SERVICE_RETENTION_APPLY_ON_POLICY_CHANGE=true`). Set `ENTITY_SERVICE_APPLY_RETENTION_BOOTSTRAP=true` to force a full-database batch apply even when no policy ingest changed.
2. **Ingest**: On every ingest, if an existing aspect got updated, it applies the retention policy to the urn-aspect pair being ingested.

We are planning to support a cron-based application of retention in the near future to ensure that the time-based retention is applied correctly.

### System-update environment variables

| Variable                                                 | Default | Purpose                                                                                          |
| -------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------ |
| `ENTITY_SERVICE_ENABLE_RETENTION`                        | `true`  | Master switch for retention enforcement                                                          |
| `ENTITY_SERVICE_RETENTION_APPLY_ON_POLICY_CHANGE`        | `true`  | Batch-apply retention to stored aspects when a policy is updated during system-update            |
| `ENTITY_SERVICE_APPLY_RETENTION_BOOTSTRAP`               | `false` | Force a full-database batch apply on every system-update, even if no policy changed              |
| `ENTITY_SERVICE_RETENTION_OVERWRITE_NON_SYSTEM_POLICIES` | `false` | Overwrite retention policies last written by a user/API (not the system actor) when YAML differs |

If desired YAML differs from a user-managed stored policy and overwrite is not enabled, system-update logs a **warning** with remediation steps (`forceOverwrite: true` on the YAML entry, the env var above, or an API update). The upgrade step still succeeds.

## How to configure?

We have enabled with feature by default. Please set **ENTITY_SERVICE_ENABLE_RETENTION=false** when
creating the datahub-gms container/k8s pod to prevent the retention policies from taking effect.

When system-update runs (datahub-upgrade), retention policies are loaded as follows:

1. First, the default bundled **version-based** retention (e.g. keep **20 latest aspects** for all entity-aspect pairs, and tighter policies for specific status aspects).
2. Second, YAML files from the plugin path (`datahub.plugin.retention.path`, e.g. `/etc/datahub/plugins/retention`) are read and merged with the default set.

For docker, we set docker-compose to mount `${HOME}/.datahub` directory to `/etc/datahub` directory
within the containers, so you can customize the initial set of retention policies by creating
a `${HOME}/.datahub/plugins/retention/retention.yaml` file.

We will support a standardized way to do this in Kubernetes setup in the near future.

The format for the YAML file is as follows:

```yaml
- entity: "*" # denotes that policy will be applied to all entities
  aspect: "*" # denotes that policy will be applied to all aspects
  config:
    retention:
      version:
        maxVersions: 20
- entity: "dataset"
  aspect: "datasetProperties"
  config:
    retention:
      version:
        maxVersions: 20
      time:
        maxAgeInSeconds: 2592000 # 30 days
- entity: "dataHubExecutionRequest"
  aspect: "dataHubExecutionRequestResult"
  forceOverwrite: true # overwrite user-managed policies for this entry; still skips if config unchanged
  config:
    retention:
      version:
        maxVersions: 1
```

Changing plugin YAML and re-running system-update re-applies policies when the config differs (no restart required for policy ingest). Routine changes to bundled defaults (e.g. `*/*` `maxVersions`) do not require `forceOverwrite` if the stored policy was written by the system actor.

Note, it searches for the policies corresponding to the entity, aspect pair in the following order:

1. entity, aspect
2. \*, aspect
3. entity, \*
4. _, _

Re-run non-blocking system-update after creating or changing plugin YAML to ingest and apply policies.
