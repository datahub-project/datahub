# Configuring Database Retention

## Goal

DataHub uses a database (or key-value store) to store different versions of the aspects as they get ingested. Storing
multiple versions of the aspects allows us to look at the history of how the aspect changed and to rollback to previous
version when incorrect metadata gets ingested. However, each version takes up space in the database, while bringing less
value to the system. We need to be able to impose **retention** on these records to keep the size of the DB in check.

Goal of the retention system is to be able to **configure and enforce retention policies** on documents in various
levels (
global, entity-level, aspect-level)

## What type of retention policies are supported?

We support 3 types of retention policies.

1. Indefinite retention: Keep all versions of aspects
2. Version-based retention: Keep the latest N versions
3. Time-based retention: Keep versions that have been ingested in the last N seconds

Note, the latest version of each aspect (version 0) is never deleted. This is to ensure that we do not impact the core
functionality of DataHub while applying retention.

## When is the retention policy applied?

As of now, retention policies are applied in two places

1. **GMS boot-up**: On boot, it runs a bootstrap step to ingest the predefined set of retention policies. If there were
   no existing policies or the existing policies got updated, it will trigger an asynchronous call to apply retention
   to **
   all** records in the database.
2. **Ingest**: On every ingest, if an existing aspect got updated, it applies retention to the urn, aspect pair being
   ingested.

We are planning to support a cron-based application of retention in the near future to ensure that the time-based
retention is applied correctly.

## How to configure?

For the initial iteration, we have made this feature opt-in. Please set **ENTITY_SERVICE_ENABLE_RETENTION=true** when
creating the datahub-gms container/k8s pod.

On GMS start up, it fetches the list of retention policies to ingest from two sources. First is the default we provide,
which adds a version-based retention to keep 20 latest aspects for all entity-aspect pairs. Second, we read YAML files
from the `/etc/datahub/plugins/retention` directory and overlay them on the default set of policies we provide.

For docker, we set docker-compose to mount `${HOME}/.datahub/plugins` directory to `/etc/datahub/plugins` directory
within the containers, so you can customize the initial set of retention policies by creating
a `${HOME}/.datahub/plugins/retention/retention.yaml` file.

We will support a standardized way to do this in kubernetes setup in the near future. 

The format for the YAML file is as follows. 

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

Note, it searches for the policies corresponding to the entity, aspect pair in the following order
1. entity, aspect
2. *, aspect
3. entity, *
4. *, *

By restarting datahub-gms after creating the plugin yaml file, the new set of retention policies will be applied. 