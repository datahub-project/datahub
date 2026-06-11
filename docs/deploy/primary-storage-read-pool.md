---
description: "Configure optional read pools for GMS entity-aspect storage (Ebean or Cassandra) to offload read traffic from the primary database."
---

# Primary storage read pool

GMS can maintain a **second connection pool** (Ebean) or **Cassandra session** (read pool) for
entity-aspect reads, while all writes and locking reads stay on the **primary** pool. This is
scoped to the entity aspect DAO only — not Elasticsearch, Kafka consumers, pgQueue, or upgrade
jobs.

For the full list of environment variables, see [Environment Variables](environment-vars.md).
For metrics, see [Monitoring — Primary storage read pool](../advanced/monitoring.md#primary-storage-read-pool-metrics).

## When to use it

| Goal                                                          | Recommended mode                                                                               |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Isolate read connection usage from write pool (same database) | **Split-pool** — same URL/hosts, two pools                                                     |
| Offload reads to a physical read replica                      | **Replica** — set `EBEAN_READ_POOL_URL` or `CASSANDRA_READ_POOL_HOSTS` to the replica endpoint |
| Entire GMS instance must not write metadata                   | Use `DATAHUB_READ_ONLY=true` (separate from read pool; see below)                              |

**Split-pool** does not avoid replica lag (there is no replica). It only separates connection
limits and can reduce contention on the primary pool.

**Replica** mode also isolates in-process entity cache keys by read preference so primary and
replica reads do not share the same cache entry.

## What is routed

Routing is driven by `OperationContext` and `PrimaryStorageResolver`:

| Operation                                                                        | Pool                                 |
| -------------------------------------------------------------------------------- | ------------------------------------ |
| Writes, transactions, `FOR UPDATE` reads                                         | **PRIMARY** (always)                 |
| `batchGet`, `getAspect`, `getAspectsInRange`, timeline reads (`forUpdate=false`) | **READ** when enabled and registered |
| `opContext == null` or READ pool not registered                                  | **PRIMARY** (safe fallback)          |

Consumers and jobs that construct their own `OperationContext` should use `ReadPreference.PRIMARY`
when read-your-writes matters (for example ingestion paths).

## Ebean (MySQL / PostgreSQL)

Enable on GMS when `entityService.impl` is `ebean` (default):

```bash
export EBEAN_READ_POOL_ENABLED=true
# Optional — omit for split-pool (defaults to primary URL):
# export EBEAN_READ_POOL_URL=jdbc:postgresql://reader.example.com:5432/datahub
```

Spring maps `ebean.readPool.*` in `application.yaml` to the variables above.

The read pool datasource is configured with **JDBC read-only** connections (`readOnly=true`,
`autoCommit=true`). Mutations attempted on that pool fail at the driver level.

## Cassandra

Enable on GMS when `entityService.impl=cassandra`:

```bash
export CASSANDRA_READ_POOL_ENABLED=true
# Optional — omit for split-pool (defaults to primary hosts):
# export CASSANDRA_READ_POOL_HOSTS=replica-host1,replica-host2
```

Unlike EBean, Cassandra has no driver-level read-only session mode. Routing uses a separate
`CqlSession` (split-pool or replica contact points). Use Cassandra roles with SELECT-only grants on
replica credentials when you need database-enforced read access.

## Interaction with `DATAHUB_READ_ONLY`

`DATAHUB_READ_ONLY=true` disables writes on entity aspect DAOs and **does not register** the read
pool even if `*_READ_POOL_ENABLED=true`. Do not enable the read pool expecting it to replace
read-only mode — they solve different problems.

After changing read pool or read-only settings, restart GMS (for example
`scripts/dev/datahub-dev.sh env restart`).

## Observability

GMS emits Micrometer counters when metrics are enabled:

- `primary_storage_target_used` — tags: `target` (`PRIMARY` \| `READ`), `store` (`ebean` \|
  `cassandra`), `forUpdate`
- `primary_storage_read_fallback_to_primary` — incremented when the context prefers READ but no
  read pool is registered

## Verification

Integration tests (Testcontainers, split-pool, no separate replica container):

- `PrimaryStorageReadPoolPostgresIT` — PostgreSQL + Ebean
- `PrimaryStorageReadPoolCassandraIT` — Cassandra

Run:

```bash
./gradlew :metadata-io:test \
  --tests com.linkedin.metadata.entity.storage.PrimaryStorageReadPoolPostgresIT \
  --tests com.linkedin.metadata.entity.cassandra.PrimaryStorageReadPoolCassandraIT
```

## Out of scope

The following continue to use the **primary** Ebean `Database` or Cassandra session only:

- MCE/MAE consumer jobs with their own Ebean configuration
- pgQueue and SQL upgrade paths
- `datahub-upgrade` restore/load jobs
- Listing, streaming, and migration DAO APIs
