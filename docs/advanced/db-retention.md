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

1. **System-update (datahub-upgrade)**: The default retention policies are ingested when you run the non-blocking system-update (e.g. `IngestRetentionPolicies` step). The default YAML is bundled in the datahub-upgrade module (`boot/retention.yaml`). If no policy existed before or the existing policy was updated and `entityService.retention.applyOnBootstrap` is true, a batch apply runs to apply the policy (or policies) to **all** records in the database.
2. **Ingest**: On every ingest, if an existing aspect got updated, it applies the retention policy to the urn-aspect pair being ingested.

We are planning to support a cron-based application of retention in the near future to ensure that the time-based retention is applied correctly.

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
```

Note, it searches for the policies corresponding to the entity, aspect pair in the following order:

1. entity, aspect
2. \*, aspect
3. entity, \*
4. _, _

By restarting datahub-gms after creating the plugin yaml file, the new set of retention policies will be applied.
