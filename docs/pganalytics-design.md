---
title: "pgAnalytics: PostgreSQL Analytics Store"
---

# pgAnalytics: Multi-Source Analytics Store

## Purpose

pgAnalytics is an optional PostgreSQL store for **product and operational analytics**:

- **`datahub_usage`** — UI/product usage events (raw + chart rollups)
- **`api_usage`** — request/aggregation flush metrics from the OSS usage metric registry (including MAU distincts)
- **`system_usage`** — inventory gauges (entity counts)

It is **not** the same subsystem as [pgTimeseries](./pgtimeseries-design.md) (aspect history). Domains stay separate; ops patterns (partman, pools, SqlSetup, multi-store registry) are mirrored.

Durable aggregation lives in Postgres only. This feature does **not** add a new GraphQL analytics API or an external publish path.

## Modes

| Mode                       | Config                                                         | Behavior                                                                |
| -------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **Disabled (default)**     | `DATAHUB_PGANALYTICS_ENABLED=false`                            | No analytics DDL/pool                                                   |
| **Schema + dual path**     | `enabled=true`, usage-events still `elasticsearch`             | SqlSetup creates tables; product charts stay on search until SoT switch |
| **Postgres SoT (product)** | `enabled=true`, `DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres` | MAE indexes into `*_event`; GraphQL product charts use JDBC             |
| **api_usage flush**        | `DATAHUB_PGANALYTICS_API_USAGE_FLUSH_ENABLED=true`             | `UsageFlushSink` merges into UTC-hour rollups + distinct sidecars       |
| **Entity counts**          | `DATAHUB_PGANALYTICS_ENTITY_COUNT_SINK_ENABLED=true`           | `EntityCountMetricsSink` writes `latest` hourly gauges                  |

## Storage model

| Table                   | Role                                                                                           |
| ----------------------- | ---------------------------------------------------------------------------------------------- |
| `{prefix}_event`        | Raw facts (primarily `datahub_usage`), RANGE on `event_time`                                   |
| `{prefix}_rollup`       | Hour/day/month aggregates (`grain`, `metric_family`, `metric_name`, `merge_kind`, `group_key`) |
| `{prefix}_distinct_set` | Identity sidecars for `merge_kind=distinct` (MAU)                                              |
| `{prefix}_watermark`    | Seal ledger for progressive compaction                                                         |

**Merge kinds** (from the usage metric registry): `additive`, `distinct`, `latest`.

**Seal rule:** an hour is sealable when `now >= hour_end + input_lag` (default 900s). Open hours are never watermarked. Day/month compactors only read sealed children.

**Query rule:** product charts use rollups only when requested grain is **≥ hour** and the range is fully sealed; otherwise raw within retention. Sub-hour historical charts require raw retention.

## Progressive compaction

Control plane is **store-agnostic**; pgAnalytics is the only backend today.

| Layer               | Component                                    | Notes                                                                                                                     |
| ------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Scheduler           | SYSTEM source `datahub-analytics-compaction` | Bootstrap MCP; default cron `0 * * * *` UTC (hourly); CLI version defaults to `bundled`; configurable via source schedule |
| API                 | `POST /openapi/operations/analytics/compact` | Budgets + `moreWorkRemaining`; **503** when no backend registered                                                         |
| Service             | `AnalyticsCompactionService`                 | Impl: pgAnalytics (`PostgresAnalyticsCompactionService`)                                                                  |
| Partition retention | SqlSetup → `pg_cron` / partman               | `DATAHUB_PGANALYTICS_MAINTENANCE_CRON_ENABLED` — not the same as compaction                                               |

**Compaction steps (pgAnalytics impl):**

1. Seal closed hours (materialize `datahub_usage` `event_count` by `event_type` into hourly rollups; watermark all families)
2. Compact sealed hour → day (sum additive; union distincts; last-wins latest)
3. Compact sealed day → month
4. Partman retention (separate job)

**Seal semantics (impl):**

- `datahub_usage`: materialize from raw (exclude `usage_source=backend`), then watermark
- `api_usage` / `system_usage`: hour rows from flush/entity sinks when enabled; seal = watermark only

**Coordination:** Postgres session `pg_try_advisory_lock(hashtext('datahub_analytics_compact'))` in the impl. Soft-skip / `lockNotAcquired` if contended. SYSTEM source soft-skips on HTTP 503 (e.g. MySQL stacks).

**Load budgets** (generic; defaults via `DATAHUB_ANALYTICS_COMPACT_*`):

| Budget               | Default | Effect                                                   |
| -------------------- | ------- | -------------------------------------------------------- |
| `maxHoursToSeal`     | 6       | Cap hours sealed per call                                |
| `maxDaysToCompact`   | 2       | Cap hour→day per call                                    |
| `maxMonthsToCompact` | 1       | Cap day→month per call                                   |
| `maxWallClockMillis` | 30000   | Soft stop; unlock in `finally`; `moreWorkRemaining=true` |

Catch-up spans hourly ticks. One compact HTTP call per SYSTEM run.

## Configuration

```bash
DATAHUB_PGANALYTICS_ENABLED=true
DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres
DATAHUB_PGANALYTICS_MAINTENANCE_CRON_ENABLED=true
# Optional sinks (application.yaml defaults off; Postgres Compose profiles default on):
# DATAHUB_PGANALYTICS_API_USAGE_FLUSH_ENABLED=true
# DATAHUB_PGANALYTICS_ENTITY_COUNT_SINK_ENABLED=true

# Compaction budgets (generic API / SYSTEM source):
# DATAHUB_ANALYTICS_COMPACT_MAX_HOURS=6
# DATAHUB_ANALYTICS_COMPACT_MAX_DAYS=2
# DATAHUB_ANALYTICS_COMPACT_MAX_MONTHS=1
# DATAHUB_ANALYTICS_COMPACT_WALL_CLOCK_MILLIS=30000
```

Optional multi-store overlay: `DATAHUB_PGANALYTICS_CONFIG_FILE` (Spring resource URI). Routing keys are **`metric_family`** values (`datahub_usage`, `api_usage`, `system_usage`).

Registry metrics for `system_usage` / `datahub_usage` live alongside `api_usage` in
`usage_metric_registry.yaml`.

## Docker Compose

Postgres quickstart/debug profiles enable **exclusive** pgAnalytics product SoT by default
(`DATAHUB_PGANALYTICS_ENABLED=true`, `DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres`). Override
`DATAHUB_USAGE_EVENTS_IMPLEMENTATION=elasticsearch` to keep the search index as SoT.
They also default `DATAHUB_PGANALYTICS_API_USAGE_FLUSH_ENABLED=true` and
`DATAHUB_PGANALYTICS_ENTITY_COUNT_SINK_ENABLED=true` so api_usage (including active-identity
distincts) and system entity-count gauges land in Postgres.

Compaction is enabled via bootstrap of SYSTEM source `datahub-analytics-compaction` (hourly). Compose
postgres stacks include **actions + GMS** so the source can call the compact API. The actions image
bundles a venv for `datahub-analytics-compaction`. Non-postgres stacks soft-skip when the backend
is unavailable.

Pool sizing knobs (`DATAHUB_PGANALYTICS_MIN_CONNECTIONS`, `DATAHUB_PGANALYTICS_MAX_CONNECTIONS`,
`DATAHUB_PGANALYTICS_WAIT_TIMEOUT_MILLIS`, and related idle/age/leak settings) mirror the
pgTimeseries pool. Default max remains 12; raise only if concurrent chart queries contend with
ingestion under load. Seal checks batch watermark lookups so chart paths do not open one connection
per hour/day bucket.

## Smoke / SoT testing

- Tracking OpenAPI smoke (`tests/openapi/v1/test_tracking.py`) branches on
  `DATAHUB_USAGE_EVENTS_IMPLEMENTATION`: under `postgres`, assert inserts into
  `metadata_analytics_event` and that the ES `datahub_usage_event` index stays empty.
- Analytics chart smoke (`tests/analytics/`) loads fixture events via
  `backfill_activity_events.py --load-to-postgres` when SoT is postgres (ES load path remains for
  elasticsearch SoT). Charts read JDBC, so ES-only fixtures leave required charts empty under
  exclusive postgres SoT.
