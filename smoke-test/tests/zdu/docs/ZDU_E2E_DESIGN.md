# Zero-Downtime Upgrade (ZDU) — End-to-End Test Framework Design

| Field        | Value                                                                                                                                                                       |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Author**   | Govind Mishra                                                                                                                                                               |
| **Status**   | Review pending                                                                                                                                                              |
| **Audience** | Platform                                                                                                                                                                    |
| **Scope**    | Single, self-contained design doc covering every aspect of the ZDU end-to-end test framework — problem, goals, architecture, phases, scenarios, operations, and extensions. |

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Background — ZDU on Kubernetes](#3-background--zdu-on-kubernetes)
4. [Kubernetes-to-Docker Conversion (10 Phases)](#4-kubernetes-to-docker-conversion-10-phases)
5. [Detailed Test Phases](#5-detailed-test-phases)
6. [Framework Architecture](#6-framework-architecture)
7. [Component Responsibilities](#7-component-responsibilities)
8. [Test Suites (A–I)](#8-test-suites-ai)
9. [Test Scenario Sheet](#9-test-scenario-sheet)
10. [Cross-Cutting Concerns](#10-cross-cutting-concerns)
11. [Operating the Framework](#11-operating-the-framework)
12. [Implementation Status & Roadmap](#12-implementation-status--roadmap)
13. [Future Extensions](#13-future-extensions)
14. [Success Criteria](#14-success-criteria)
15. [Glossary](#15-glossary)

---

## 1. Problem Statement

Zero-Downtime Upgrade (ZDU) is the composition of four tightly coupled subsystems that must
execute end-to-end while DataHub continues serving live read and write traffic, without causing
data loss, schema or shape regressions, or violations of ordering guarantees:

1. **Elasticsearch incremental reindexing (Phase 1)** — reindex into a new physical index and
   atomically swap the alias.
2. **Kubernetes rolling restarts with dual-write enabled** — old and new GMS coexist briefly; new
   writes fan out to both old and new ES indices for rollback safety.
3. **Elasticsearch catch-up reindexing (Phase 2)** — backfill the `[T0, T1]` gap window into the
   new index and optionally disable dual-write.
4. **Aspect schema migration via `AspectMigrationMutatorChain`** — runtime mutators upgrade
   aspects on read and on write; a background sweep migrates the long tail in the database.

Unit tests validate the chain and individual mutators in isolation. **End-to-end coverage is
missing** — there is no harness that proves legacy data migrates correctly, that consistency
holds during the sweep, or that downstream systems (Elasticsearch, MySQL) converge on the new
shape under live traffic.

This framework closes that gap.

---

## 2. Goals & Non-Goals

### Goals

1. **Simulate a real DataHub Helm upgrade end-to-end** in a Docker Compose harness — from
   "old version running" through "new version running with all data migrated".
2. **Cover every ZDU surface** in one harness:
   - Aspect schema migration: read mutator, write mutator, background sweep, gap bridging.
   - ES Phase 1: incremental reindex into a new physical index with alias swap.
   - ES rollback dual-write window: writes go to both old and new indices for safety.
   - ES Phase 2 catch-up: backfill the `[T0, T1]` gap window into the new index.
   - On-the-fly mutator chain disable after sweep completion.
3. **Validate invariants under live traffic** — concurrent reads, writes, and migrations must
   not lose data, return wrong shapes, or break ordering guarantees.
4. **Resumability and idempotency** for every phase that can be interrupted (Phase 1 reindex
   polling, Phase 2 catch-up cursor, sweep cursor).
5. **Failure-mode coverage** — failed reindex, failed catch-up, rollback from new → old version
   mid-migration, MAE OOM, ES network partition, concurrent upgrade jobs.
6. **Single command to run** locally + CI-ready exit codes + machine-readable JSON report.
7. **Scenario-driven** — new test cases are added by appending a row to a Google Sheet (with a
   committed local CSV fallback). No code change required for a new scenario in the same suite.

### Non-Goals

- **Performance / load testing.** This framework verifies correctness, not throughput. Volume
  tests are a separate effort.
- **K8s rolling restart simulation.** Docker Compose can model upgrade-job sequencing, but it
  cannot reproduce K8s-native rolling pod restarts. We approximate this with a sequenced
  `restart` step. A KIND-based Suite I is optional and deferred.
- **Cross-cloud testing.** AWS-specific paths (RDS snapshots, OpenSearch native replication) are
  out of scope. The framework targets application-layer ZDU behavior, which is cloud-agnostic.
- **Browser / UI compatibility.**
- **Multi-tenant scenarios** — single-tenant only.
- **Aspect schema downgrade / rollback.** Suite G covers ES rollback to an old GMS image; that
  is the dual-write safety net, not aspect schema rollback.

---

## 3. Background — ZDU on Kubernetes

A real production ZDU is the composition of four independent migration tracks that all happen
inside one Helm upgrade.

### 3.1 Phase 1 — Blocking (Pre-Hook)

Runs before rollout and blocks deployment.

1. **Pre-hook job (`datahub-upgrade.jar`)**
   - Reindexes Elasticsearch into a new physical index per affected alias.
   - Applies schema and mapping changes.
2. **Kubernetes rolling restart**
   - GMS, MAE, MCE, frontend pods restart in sequence.
3. **Dual-write window opens**
   - From the moment the alias swaps until the new MAE consumer starts, writes from the old GMS
     keep landing in the old physical index. This is the `[T0, T1]` gap.

### 3.2 Phase 2 — Non-Blocking (Post-Hook)

Runs after pods are healthy. Does not block traffic.

- Trigger: `datahub-upgrade.jar -u SystemUpdateNonBlocking`.
- Background execution via Spring-gated steps.

Steps:

1. **Replay missed writes (MAE catch-up)**
   - Gap: writes between T0 (alias switch) and T1 (MAE restart on new code).
   - Fix: replay as `RESTATE` events through Kafka, dual-writing to old + new indices for
     rollback safety.
2. **Background schema migration (sweep)**
   - Gradual aspect migration to the new schema via the mutator chain.
3. **Runtime migration (on-the-fly)**
   - Old-format reads are converted on-the-fly via the read mutator.
   - All new writes use the updated schema via the write mutator.

### 3.3 Track ↔ Suite Map

| Track                     | What changes                                                                      | Test surface   |
| ------------------------- | --------------------------------------------------------------------------------- | -------------- |
| Aspect schema migration   | `systemMetadata.schemaVersion` bumped on rows; mutator chain transforms shape     | Suites A, E, F |
| ES Phase 1 (blocking)     | `BuildIndicesIncrementalStep` reindexes into a `next` physical index, swaps alias | Suite B        |
| ES dual-write (rollback)  | `UpdateIndicesUpgradeStrategy` writes to both old and new indices in `[T0, T1]`   | Suite C        |
| ES Phase 2 (non-blocking) | `IncrementalReindexCatchUpStep` backfills `[T0, T1]` gap; optional dual-write off | Suite D        |

These tracks share a control plane (`DataHubUpgradeResult` aspect on `urn:li:dataHubUpgrade:...`),
share the Kafka/MAE pipeline, and overlap in time. Bugs in any one — or in their interactions —
cause data loss, read inconsistency, or upgrade failures only discovered in production.

---

## 4. Kubernetes-to-Docker Conversion (10 Phases)

To replicate every Kubernetes ZDU step inside Docker Compose, each Helm pre-hook, rolling
restart, and post-hook is converted into a discrete sequential phase in the test runner.

| #   | Phase                 | Purpose                                                                                                                |
| --- | --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| 1   | `discovery`           | Snapshot service images; verify GMS health; confirm ES connectivity.                                                   |
| 2   | `seed`                | Seed scenario entities to a known baseline state (DB-direct + GMS-route).                                              |
| 3   | `snapshot_t0`         | Capture pre-upgrade baseline: ES index names, doc counts, schema versions, T0 epoch, MySQL aspect rows.                |
| 4   | `upgrade_blocking`    | Run `system-update -u SystemUpdateBlocking`; capture per-index state from `DataHubUpgradeResult`.                      |
| 5   | `inject_traffic_pre`  | Between Phase 1 completion and rolling restart: write via OLD GMS to validate ingestion into the legacy index (T0–T1). |
| 6   | `rolling_restart`     | Sequenced restart: GMS → MAE → MCE (`compose up --build`). Validate MAE initializes `UpdateIndicesUpgradeStrategy`.    |
| 7   | `inject_traffic_dual` | Write via NEW GMS during dual-write mode; ensure data lands in BOTH legacy and new physical indices.                   |
| 8   | `upgrade_nonblocking` | Run `system-update -u SystemUpdateNonBlocking` while concurrent IO continues (sweep + catch-up + dual-write disable).  |
| 9   | `runtime_migration`   | Validate on-the-fly read and write mutators while the non-blocking phase progresses.                                   |
| 10  | `validation`          | Final correctness check: schema versions, ES doc counts, alias mappings, dual-write disabled.                          |

---

## 5. Detailed Test Phases

### 5.1 Phase 1 — Discovery

Captures the current "old" state before anything is touched. Confirms GMS and Elasticsearch are
reachable. If GMS is unreachable, the runner aborts — there is no point seeding into a down
system.

**Outputs:** image tag per service; GMS URL confirmed healthy; ES reachable.

### 5.2 Phase 2 — Seed

Writes test entities using the old GMS, **intentionally omitting `schemaVersion` from
`systemMetadata`**. This creates the conditions the sweep is designed to fix — data in the DB
that predates ZDU.

**Key constraint:** `ASPECT_MIGRATION_MUTATOR_ENABLED` must be `false` on the currently running
GMS (default). If the flag is already `true`, the write-path mutator immediately upgrades the row
on write and the sweep finds nothing to migrate. The framework warns and continues; validation
will trivially pass without exercising the migration path.

For pre-Phase-1 data in a known ES state, **IO-pool entities are inserted directly into MySQL**
(`metadata_aspect_v2` with `systemmetadata='{}'`) so they always land at `schemaVersion=null`
(v1) regardless of `ASPECT_MIGRATION_MUTATOR_ENABLED`.

**Outputs:** seeded entity list, expected post-migration `schemaVersion`, and a data-shape
validator function per entity.

### 5.3 Phase 3 — Snapshot T0

Captures pre-upgrade state for later validation:

- ES index names and aliases (`_cat/indices`, `_cat/aliases`).
- Per-index document counts.
- MySQL aspect counts at each `schemaVersion`.
- T0 epoch (used for `[T0, T1]` window assertions).
- `DataHubUpgradeResult` aspect — must be absent or in a terminal state for the upcoming version.

### 5.4 Phase 4 — Upgrade Blocking

Runs `docker compose run --rm system-update-debug -u SystemUpdateBlocking` with the **new**
image. This corresponds to the blocking half of a real Helm upgrade — everything that must
finish before pods can roll.

The framework watches `BuildIndicesIncrementalStep` events:

- `Reindexing index '{x}' -> '{next}' with task {taskId}`
- `Polling task {taskId}: status={status}, completed={n}/{total}`
- `Alias swapped: {alias} -> {nextIndex}`

It then captures `DataHubUpgradeResult.indicesState[*]` for use by downstream phases.

### 5.5 Phase 5 — Inject Traffic Pre

Between Phase 1 completion and the rolling restart, the **old** GMS is still running but ES has
already swapped the alias. Writes from the old GMS continue to land in the **old physical
index** via direct index-name resolution that bypasses the alias.

This phase fires writes via the old GMS and verifies the writes land in the old index. These
become the "T0–T1 gap" entities that Phase 2 catch-up must backfill.

### 5.6 Phase 6 — Rolling Restart

Sequenced restart of GMS → MAE → MCE using `docker compose up --build -d {service}` per service.

Verifies the new MAE initializes `UpdateIndicesUpgradeStrategy` by tailing the log line:
`Recorded dual-write start time for index '{name}'`.

**Note:** This approximates the K8s rolling pod restart. K8s-true rolling is out of scope; what
we get is sequencing correctness, not partial-replica behavior.

### 5.7 Phase 7 — Inject Traffic Dual

Fires writes via the **new** GMS during the dual-write window. Asserts dual-writes hit both the
old and new physical indices via direct ES queries.

This is the rollback safety net validation — if a rollback to the old GMS is needed during the
next 24 hours, the old GMS must still find data via the old index.

### 5.8 Phase 8 — Upgrade Non-Blocking

The most complex phase. Runs `docker compose run --rm system-update-debug -u SystemUpdateNonBlocking`
while concurrent reader and writer threads continue to hit the system. Responsible for:

1. **`MigrateAspectsStep`** — the aspect schema sweep. Reads every `embed` and `globalTags`
   aspect from the DB and upgrades through the mutator chain. Concurrent writes may race with
   the sweep (see "Deterministic Race Window" below).
2. **`IncrementalReindexCatchUpStep`** — backfills the `[T0, T1]` gap window into the new index.
3. **`DUAL_WRITE_DISABLED` marking** — for indices where rollback dual-write is no longer
   needed, flips the per-index status. The next MAE state-poller cycle picks this up and stops
   dual-writing.
4. **Chain disable** — after the full sweep completes, `AspectMigrationMutatorChain.disable()`
   is called so subsequent reads bypass mutators.

#### 5.8.1 Deterministic Race Window

The framework guarantees that a concurrent client write lands **inside** the sweep's
read→write gap on every batch — not just probabilistically. This prevents overwrites of any
aspects that were modified while the background sweep was migrating them.

Test config: `pre_write_delay_ms = 500` →
`SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS=500` is passed to the upgrade container.
`MigrateAspectsStep` reads it via `System.getenv()`.

Inside each sweep batch:

```
MigrateAspectsStep:
  log.info("Processing batch URNs: urn:li:dashboard:(test,zdu-io-pool-0),...")  ← always logged
  Thread.sleep(500ms)   ← race window is open (only when PRE_WRITE_DELAY_MS > 0)
  entityService.ingestProposal(...)

LogMonitor._tail_popen (Python, reading upgrade container stdout):
  parses "Processing batch URNs" line → SweepState.BATCH_URNS event

Test writer thread (consuming BATCH_URNS events):
  ← sees BATCH_URNS event with [urn:li:dashboard:(test,zdu-io-pool-0), ...]
  ← fires ingest_mcp() to each IO-pool URN immediately (~10–50 ms)
  ← write completes; entity DB version bumped to N+1
  ← sleep ends; sweep calls ingestProposal with If-Version-Match: N
  ← ConditionalWriteValidator: stored N+1 ≠ header N → REJECTS sweep write (silent)
```

The client write always wins. `ConditionalWriteValidator` silently skips the stale sweep write
(non-API context — no exception, no crash). `[upgrade] sweep URN:` and `[gms] Producing MCL …`
lines share the same URN, making the race visible line-by-line in the test output.

### 5.9 Phase 9 — Runtime Aspect Read and Write (Mutator Chain)

While the non-blocking phase is in progress, the framework drives reads and writes through the
running GMS and validates that:

- Reads of pre-ZDU rows return the upgraded shape via the read-path mutator (DB row may still be
  stale).
- Writes from clients persist as the latest schema version via the write-path mutator,
  regardless of sweep state.
- After the chain disables, subsequent reads bypass mutators and return the raw stored shape.

### 5.10 Phase 10 — Validation

Asserts correctness across **five dimensions**:

1. **Per-entity schema version** — every seeded entity must be at its expected `schemaVersion`.
   The data shape must match the post-migration validator.
2. **Write path** — every entity written by a writer thread during the sweep must have received
   the current schema version on the first write. No entity should require a sweep to reach the
   correct version after being written by a fully-upgraded GMS.
3. **Read progression** — reader observations are grouped by URN and sorted by timestamp; no URN
   may move backward in `schemaVersion`. A row observed at v3 must never subsequently be observed
   at v2.
4. **Elasticsearch field presence** — for scenarios with an ES check, query the search index to
   confirm migrated fields appear in results and the index mapping was updated.
5. **Alias targets and dual-write state** — alias for each index points at the expected physical
   index; per-index `dualWriteStartTime` is set or absent according to expectation; indices
   marked `DUAL_WRITE_DISABLED` when the rollback flag is off.

The validation phase **does not fail fast** — it collects all failures and reports them
together.

**Failure output format:**

```
FAILED [TC-011]: urn:li:dataset:(urn:li:dataPlatform:mysql,zdu-test-2,PROD) embed
  Expected: schemaVersion=4
  Got:      schemaVersion=2
  Reason:   bridgeGap() not applied — V2→V3 mutator missing and gap not bridged
```

---

## 6. Framework Architecture

### 6.1 High-Level Topology

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  Test Orchestrator (pytest + framework/runner.py)                │
  │  ┌────────────────────────────────────────────────────────────┐  │
  │  │ Phase Pipeline                                             │  │
  │  │  discovery → seed → snapshot_t0 → ┐                        │  │
  │  │                                   ├→ upgrade_blocking      │  │
  │  │                                   │   (system-update job)  │  │
  │  │                                   ├→ inject_traffic_pre    │  │
  │  │                                   ├→ rolling_restart       │  │
  │  │                                   ├→ inject_traffic_dual   │  │
  │  │                                   ├→ upgrade_nonblocking   │  │
  │  │                                   ├→ runtime_migration     │  │
  │  │                                   └→ validation            │  │
  │  └────────────────────────────────────────────────────────────┘  │
  └────────┬──────────────┬──────────────┬─────────────┬────────────┘
           │              │              │             │
           ↓              ↓              ↓             ↓
   ┌──────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
   │ DockerCompose│ │ DataHub    │ │ ES Client  │ │ Log Monitor│
   │ controller   │ │ HTTP client│ │ (raw API)  │ │ (tail logs,│
   │              │ │ (GraphQL/  │ │            │ │  regex     │
   │              │ │  REST/MCP) │ │            │ │  events)   │
   └──────────────┘ └────────────┘ └────────────┘ └────────────┘
           │              │              │             │
           ↓              ↓              ↓             ↓
   ┌──────────────────────────────────────────────────────────────────┐
   │  Docker Compose Stack (under test)                               │
   │  ┌────────────┐ ┌────────────┐ ┌──────────────┐ ┌──────────┐    │
   │  │ MySQL      │ │ Kafka      │ │ Elasticsearch│ │ Schema   │    │
   │  └────────────┘ └────────────┘ └──────────────┘ │ Registry │    │
   │  ┌────────────┐ ┌────────────┐ ┌──────────────┐ └──────────┘    │
   │  │ datahub-gms│ │ datahub-   │ │ datahub-mae- │ ┌──────────┐    │
   │  │ (vN-old →  │ │ mce-cons   │ │ cons (vN-old │ │ system-  │    │
   │  │  vN+1)     │ │ (vN-old →  │ │  → vN+1)     │ │ update-  │    │
   │  │            │ │  vN+1)     │ │              │ │ debug    │    │
   │  └────────────┘ └────────────┘ └──────────────┘ └──────────┘    │
   └──────────────────────────────────────────────────────────────────┘
```

### 6.2 Design Rationale: Single Phase Pipeline

Three approaches were considered:

- **Single monolithic script** — simple, but failures hard to isolate; phase ordering implicit.
  Rejected.
- **Pytest fixtures doing all the work** — fragile teardown, hard to run as a CLI without
  pytest. Rejected.
- **Phase pipeline with shared `TestContext`** _(chosen)_ — each phase has a clear input
  (context from prior phases) and output (its own context slice). Phases can be skipped
  individually for developer iteration (e.g., skip seed if data is already present). The pytest
  layer is thin — it just asserts on phase results. The same runner works as a CLI.

### 6.3 Directory Layout

```
smoke-test/tests/zdu/
├── __init__.py                         package marker
├── __main__.py                         CLI entry point (pytest-free runner)
├── conftest.py                         pytest session fixtures (scenarios, report)
├── test_zdu_upgrade.py                 one test per TC# via parametrize
├── scenarios.csv                       committed offline fallback for the Google Sheet
├── ZDU_E2E_DESIGN.md                   this document
└── framework/
    ├── __init__.py
    ├── config.py                       ZDUTestConfig + GapSimulationConfig + from_env()
    ├── context.py                      TestContext + shared dataclasses
    ├── docker_compose.py               DockerComposeClient (subprocess wrapper)
    ├── datahub_client.py               DataHubClient + AspectResponse
    ├── es_client.py                    ElasticsearchClient (search + doc count + alias state)
    ├── mysql_client.py                 MySQLClient — direct DB asserts/seeds
    ├── log_monitor.py                  LogMonitor + SweepEvent + SweepState + _parse_line()
    ├── scenario_loader.py              ZDUTestScenario + ScenarioLoader + ScenarioExecutor
    ├── reporter.py                     Reporter → JSON file + terminal summary + artifacts
    ├── runner.py                       ZDUTestRunner + ZDUReport
    ├── test_log_monitor.py             unit tests for _parse_line()
    ├── test_scenario_loader.py         unit tests for ScenarioLoader._parse_row()
    └── phases/
        ├── __init__.py
        ├── base.py                     Phase ABC + PhaseResult
        ├── discovery.py                DiscoveryPhase
        ├── seed.py                     SeedPhase
        ├── snapshot_t0.py              SnapshotT0Phase — pre-upgrade state capture
        ├── upgrade_blocking.py         UpgradeBlockingPhase — system-update Phase 1
        ├── inject_traffic.py           InjectTrafficPhase (pre / dual / post variants)
        ├── rolling_restart.py          RollingRestartPhase — sequenced GMS/MAE/MCE rebuild
        ├── upgrade_nonblocking.py      UpgradeNonBlockingPhase — Phase 2 + sweep + IO
        ├── runtime_migration.py        RuntimeMigrationPhase — on-the-fly mutator validation
        └── validation.py               ValidationPhase
```

---

## 7. Component Responsibilities

### 7.1 `DockerComposeController` — `framework/docker_compose.py`

- `up(profile=...)` / `down()` / `restart(service)`.
- `run_system_update(args, env_overrides)` — runs the upgrade job with Compose `run --rm`.
- `rebuild_service(service)` — `docker compose build {service} && docker compose up -d {service}`.
- `get_image_digest(service)` — for the discovery snapshot.
- `get_service_env(service, keys)` — reads secrets from a running container via `docker inspect`.
- `get_logs(service, since=...)` — for log-driven assertions.

### 7.2 `DataHubClient` — `framework/datahub_client.py`

Minimal REST client targeting the stable v2 aspect API. Uses only `requests`. Treats aspect data
as opaque dicts — only `systemMetadata.schemaVersion` is inspected by the framework.

- Health-poll GMS until ready.
- Write an aspect with arbitrary `systemMetadata` (callers omit `schemaVersion` to simulate
  pre-ZDU data).
- Read an aspect and expose its `schemaVersion`.

### 7.3 `ElasticsearchClient` — `framework/es_client.py`

Direct ES HTTP client (not via GMS). Required for assertions that bypass DataHub abstractions.

```python
class ElasticsearchClient:
    def list_indices(self, prefix: str) -> list[str]
    def get_alias_targets(self, alias: str) -> list[str]
    def get_doc_count(self, index_name: str) -> int
    def get_doc(self, index_name: str, doc_id: str) -> dict | None
    def get_mappings(self, index_name: str) -> dict
    def list_tasks(self, action_pattern: str) -> list[dict]   # for _reindex monitoring
```

### 7.4 `MySQLClient` — `framework/mysql_client.py`

Direct DB client for state assertions and seed-direct (bypasses GMS write path).

```python
class MySQLClient:
    def get_aspect_raw(self, urn: str, aspect: str) -> EbeanAspectV2Row | None
    def get_schema_version(self, urn: str, aspect: str) -> int | None
    def insert_aspect_raw(self, ...) -> None                              # direct DB seed
    def get_upgrade_result(self, upgrade_id_urn: str) -> dict | None      # query DataHubUpgradeResult
    def count_aspects_at_version(self, aspect: str, version: int | None) -> int
```

### 7.5 `LogMonitor` — `framework/log_monitor.py`

Tails Compose logs (`datahub-upgrade`, `datahub-gms`, `datahub-mae-consumer`) and surfaces
structured `SweepEvent` objects via a shared queue. Two daemon threads (one per container) feed
the queue; the consumer is container-agnostic.

| Pattern                                                          | Emitted by                      | Used by scenarios |
| ---------------------------------------------------------------- | ------------------------------- | ----------------- |
| `Migration sweep starting`                                       | `MigrateAspectsStep`            | Suite A           |
| `Processing batch URNs: …`                                       | `MigrateAspectsStep`            | Suites A / E / F  |
| `Sweep complete, total {n}` / `STARTED` / `COMPLETED` / `FAILED` | `MigrateAspectsStep`            | Suites A / E      |
| `Previous result was SUCCEEDED` → `SKIPPED`                      | `MigrateAspectsStep`            | TC-016 / TC-109   |
| `Alias swapped: {alias} -> {nextIndex}`                          | `BuildIndicesIncrementalStep`   | TC-101 / TC-103   |
| `Resuming polling for index {name} -> {next}`                    | `BuildIndicesIncrementalStep`   | TC-110            |
| `Recorded dual-write start time for index '{old}'`               | `UpdateIndicesUpgradeStrategy`  | TC-201            |
| `Marked index {name} as DUAL_WRITE_DISABLED`                     | `IncrementalReindexCatchUpStep` | TC-202            |
| `Catch-up for entity index {name}: window [{T0}, {T1}]`          | `IncrementalReindexCatchUpStep` | TC-301 / TC-205   |
| `Resuming catch-up for index {name} from URN: {urn}`             | `IncrementalReindexCatchUpStep` | TC-307            |
| `Removed rollback dual-write target for entity '{name}'`         | MAE state poller                | TC-206            |
| `All dual-write targets removed, shutting down state poller`     | MAE state poller                | TC-207            |
| `AspectMigrationMutatorChain disabled`                           | `AspectMigrationMutatorChain`   | TC-019 / TC-408   |

### 7.6 `ScenarioLoader` — `framework/scenario_loader.py`

Reads the Google Sheets CSV export and converts each row into a `ZDUTestScenario` object. Falls
back to a committed local CSV (`scenarios.csv`) when the sheet URL is unreachable. A **scenario
type** column dispatches to the right executor:

| Scenario type       | Executor target                                       |
| ------------------- | ----------------------------------------------------- |
| `aspect_migration`  | Single/multi-hop schema version sweep (existing path) |
| `es_phase1`         | Index reindex, alias swap                             |
| `es_phase2_catchup` | Gap catch-up validation                               |
| `dual_write`        | Rollback dual-write window                            |
| `live_traffic`      | Read/write mutator under load                         |
| `failure_recovery`  | Interrupt + resume                                    |
| `rollback`          | Old version reads dual-written old index              |

Known-failure TC numbers are hardcoded with explanations. When a TC is marked `expected_to_fail`,
the framework records `XFAIL` rather than `FAIL`. When such a TC unexpectedly passes, it records
`XPASS` (an actionable signal that the underlying issue may be fixed).

### 7.7 `ScenarioExecutor`

Bridges the scenario definition (from the sheet) and the live environment. One executor instance
is shared across all phases. For each scenario it knows how to:

- Seed the right test data at the right starting `schemaVersion`.
- Trigger the appropriate action (`sweep` / `read` / `write` / `lifecycle` / `es` / `integrity`
  / `rolling` / `phase1` / `phase2` / `dual_write` / `rollback`).
- Assert the expected result and classify the outcome as PASS / FAIL / XFAIL / XPASS / SKIP.

### 7.8 `Reporter`

Two outputs on completion:

- **Terminal summary.**
- **JSON file** at `smoke-test/build/zdu-test-report.json` — schema mirrors the existing
  `smoke-test/build/test-report.json` so the existing CI parser works unchanged.

On any phase failure, the reporter saves a failure-artifact bundle:

```
build/zdu-failure-{timestamp}/{scenario}/
├── compose-logs/                    full docker compose logs per service
├── es-cat-indices.txt               GET _cat/indices
├── es-cat-aliases.txt               GET _cat/aliases
├── upgrade-result.json              full DataHubUpgradeResult aspect content
├── mysql-aspects.csv                aspect rows for the scenario's URN range
└── reader-writer-events.jsonl       reader/writer thread observations
```

---

## 8. Test Suites (A–I)

Scenarios are grouped into **suites**. Each can run independently. CI runs all; local devs
target one suite.

### 8.1 Suite A — Aspect Schema Migration (existing TC-001 to TC-023)

| TC  | Name                          | Action    | Start | Expected | Notes                              |
| --- | ----------------------------- | --------- | ----- | -------- | ---------------------------------- |
| 001 | Full sweep single hop         | sweep     | null  | 2        |                                    |
| 002 | Read path single hop          | read      | null  | 2        | In-memory migration                |
| 003 | Write path single hop         | write     | null  | 2        |                                    |
| 004 | Full sweep multi hop          | sweep     | null  | 4        | v1→v2→v3→v4                        |
| 005 | Read path multi hop           | read      | null  | 4        |                                    |
| 006 | Write path multi hop          | write     | null  | 4        |                                    |
| 007 | Mid-chain start at v2         | sweep     | 2     | 4        | Requires `bridgeGap`               |
| 008 | Already at target v4          | sweep     | 4     | 4        | No-op                              |
| 009 | Future version v5             | sweep     | 5     | 5        | All mutators skipped               |
| 010 | Null systemMetadata           | sweep     | null  | 4        | Treated as v1                      |
| 011 | Gap in mutator chain          | sweep     | 2     | 4        | Requires `bridgeGap`               |
| 012 | transform() returns null      | sweep     | 3     | 3        | XFAIL — null is intentional no-op  |
| 013 | Invalid URN crashes sweep     | sweep     | —     | —        | XFAIL — not expected in prod       |
| 014 | Malformed JSON in metadata    | sweep     | —     | —        | XFAIL — not expected in prod       |
| 015 | ES reindexing after migration | es        | null  | 4 + ES   | Doc counts equal                   |
| 016 | Re-run after SUCCEEDED        | lifecycle | —     | —        | Sweep emits SKIPPED                |
| 017 | ABORTED treated as terminal   | lifecycle | —     | —        | ABORTED ≡ SUCCEEDED for skip logic |
| 018 | IN_PROGRESS resumes sweep     | lifecycle | —     | —        | Resumes from saved cursor          |
| 019 | chain.disable() not wired     | sweep     | —     | —        | XFAIL — known open bug             |
| 020 | Read path in-memory only      | read      | 1     | 4        | DB stays at v1; API returns v4     |
| 021 | Write path persists           | write     | 1     | 4        | Requires `bridgeGap`               |
| 022 | APP_SOURCE stamped on sweep   | integrity | —     | —        | systemMetadata.properties check    |
| 023 | Rolling upgrade               | rolling   | —     | —        | SKIP — steps not yet defined       |

### 8.2 Suite B — ES Phase 1: Incremental Reindex

Validates `BuildIndicesIncrementalStep`.

| TC  | Name                                     | Setup                                           | Action                           | Assertion                                                                                                                                                       |
| --- | ---------------------------------------- | ----------------------------------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 101 | Single-index reindex with mapping change | Seed 100 datasets in v1 mapping                 | Run `SystemUpdateBlocking`       | `next` physical index exists; alias points to `next`; doc count matches; old physical index still exists                                                        |
| 102 | No-reindex needed                        | Mappings unchanged                              | Run `SystemUpdateBlocking`       | No `next` index created; no alias change                                                                                                                        |
| 103 | Settings/mappings-only update            | Add new searchable field, compatible mapping    | Run `SystemUpdateBlocking`       | `next` index NOT created; settings/mappings applied non-disruptively                                                                                            |
| 104 | Empty source index                       | Drop all docs, then upgrade                     | Run `SystemUpdateBlocking`       | `next` empty index created; alias swapped                                                                                                                       |
| 105 | Multiple indices, all need reindex       | 3 indices with mapping changes                  | Run `SystemUpdateBlocking`       | All 3 processed sequentially; all 3 alias swaps; `DataHubUpgradeResult` has 3 `*.status=COMPLETED`                                                              |
| 106 | Mixed: 2 reindex, 1 mapping-only         | Configure accordingly                           | Run `SystemUpdateBlocking`       | 2 alias swaps; 1 in-place mapping update                                                                                                                        |
| 107 | Timeseries index reindex                 | Seed 50 dataset profile docs                    | Run `SystemUpdateBlocking`       | `next` timeseries index created; alias swapped                                                                                                                  |
| 108 | DataHubUpgradeResult state shape         | Run `SystemUpdateBlocking`                      | Inspect upgrade result aspect    | All required keys per index: `nextIndexName`, `oldBackingIndexName`, `reindexStartTime`, `sourceDocCount`, `taskId`, `requiresDataBackfill`, `status=COMPLETED` |
| 109 | Re-run after COMPLETED                   | Phase 1 already succeeded                       | Run `SystemUpdateBlocking` again | Logs show `already COMPLETED in previous run, skipping`; no second alias swap                                                                                   |
| 110 | Resume after interruption                | Kill upgrade job mid-poll                       | Restart upgrade job              | Logs show `Resuming polling for index ...`; final state COMPLETED                                                                                               |
| 111 | Reindex failure → cleanup                | Inject ES error during \_reindex                | Run `SystemUpdateBlocking`       | `next` index deleted; status=FAILED; alias unchanged                                                                                                            |
| 112 | Doc count mismatch fails alias swap      | Manually delete a doc from `next` after reindex | Run alias-swap step              | `validateAndSwapAlias` returns false; step fails; alias unchanged                                                                                               |

### 8.3 Suite C — Rollback Dual-Write

Validates `UpdateIndicesUpgradeStrategy`.

| TC  | Name                                 | Setup                                          | Action                      | Assertion                                                              |
| --- | ------------------------------------ | ---------------------------------------------- | --------------------------- | ---------------------------------------------------------------------- |
| 201 | Dual-write enabled, T1 recorded      | Phase 1 done; MAE on new code; write 1 entity  | Read `DataHubUpgradeResult` | `{indexName}.dualWriteStartTime` set within seconds of write           |
| 202 | Dual-write writes to old index       | After (201), query old physical index directly | ES `GET /{old}/_doc/{id}`   | Doc present; content matches                                           |
| 203 | Dual-write also writes to new index  | Same as (202) but new index                    | ES query                    | Doc present in `next`                                                  |
| 204 | Dual-write disabled flag respected   | `rollbackDualWriteEnabled=false`; restart MAE  | Write 1 entity              | Old index does NOT receive doc; new index does                         |
| 205 | T1 recorded only once per index      | Write same URN 5 times                         | Inspect upgrade result      | `dualWriteStartTime` matches only the first write timestamp            |
| 206 | Poller detects DUAL_WRITE_DISABLED   | After Phase 2 marks disabled                   | Wait 1 polling interval     | Logs show `Removed rollback dual-write target for entity '{name}'`     |
| 207 | Poller shuts down when targets empty | All entities marked DUAL_WRITE_DISABLED        | Wait 1 polling interval     | Logs show `All dual-write targets removed, shutting down state poller` |
| 208 | Delete propagates to old index       | Send DELETE for an entity                      | ES queries to both indices  | Both indices have doc removed                                          |

### 8.4 Suite D — ES Phase 2: Catch-Up

Validates `IncrementalReindexCatchUpStep`.

| TC  | Name                                           | Setup                                                                                                                            | Action                 | Assertion                                                            |
| --- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | -------------------------------------------------------------------- |
| 301 | Entity index gap catch-up                      | Phase 1 done; write 10 entities BEFORE rolling restart (lands in old only); start MAE on new code; run `SystemUpdateNonBlocking` | After completion       | All 10 present in `next` with new mapping fields populated           |
| 302 | Timeseries catch-up via filtered \_reindex     | Same as (301) but timeseries docs in `[T0, T1]`                                                                                  | Run non-blocking       | Filtered `_reindex` task observed; docs copied; timestamps preserved |
| 303 | Global index (graph) catch-up                  | Write entities with relationships in `[T0, T1]`                                                                                  | Run non-blocking       | Graph index has all relationships post-catchup                       |
| 304 | Global index (system metadata) catch-up        | Same with run-id metadata                                                                                                        | Run non-blocking       | System metadata index updated                                        |
| 305 | T0 >= T1 → no-op                               | Force `dualWriteStartTime <= reindexStartTime`                                                                                   | Run non-blocking       | Logs show skip; no MCLs emitted                                      |
| 306 | No Phase 1 result → no-op                      | Skip Phase 1 entirely                                                                                                            | Run non-blocking       | Step returns SUCCEEDED; no errors                                    |
| 307 | Resume from `lastUrn` checkpoint               | Interrupt mid-catchup                                                                                                            | Restart non-blocking   | Logs show `Resuming catch-up for index {name} from URN: {urn}`       |
| 308 | DUAL_WRITE_DISABLED set when rollback flag off | `rollbackDualWriteEnabled=false`; run non-blocking                                                                               | Inspect upgrade result | All COMPLETED indices marked DUAL_WRITE_DISABLED                     |
| 309 | DUAL_WRITE_DISABLED NOT set when flag on       | `rollbackDualWriteEnabled=true`; run non-blocking                                                                                | Inspect upgrade result | Status remains COMPLETED (operator must explicitly disable)          |

### 8.5 Suite E — Aspect Migration Sweep — System-Level

Beyond the existing 23 scenarios, system-level sweep validation.

| TC  | Name                                      | Setup                                                   | Action                  | Assertion                                                                                                                  |
| --- | ----------------------------------------- | ------------------------------------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| 401 | Sweep cursor resumability                 | Interrupt sweep at batch 5/20                           | Restart non-blocking    | Logs show `Resuming from createdOn >= {ms}`; sweep completes; no entity migrated twice                                     |
| 402 | Sweep respects batchDelayMs               | `delayMs=2000`, batch=10, 100 entities                  | Run sweep               | Total wall-time ≥ 18 s (9 inter-batch sleeps × 2 s)                                                                        |
| 403 | Sweep skips already-migrated rows         | Pre-migrate half the entities to target version         | Run sweep               | DB query returns only unmigrated rows; metrics show correct count migrated                                                 |
| 404 | Sweep with no mutators registered         | Empty `AspectMigrationMutatorChain`                     | Run non-blocking        | Sweep step is empty list (no-op); no errors                                                                                |
| 405 | Sweep with feature flag off               | `aspectMigrationMutatorEnabled=false`                   | Run non-blocking        | Chain disabled; sweep does not run; reads return raw data                                                                  |
| 406 | APP_SOURCE stamped on sweep writes        | Run sweep                                               | Inspect migrated aspect | `systemMetadata.properties.appSource = SYSTEM_UPDATE`                                                                      |
| 407 | IF_VERSION_MATCH header prevents stomping | Concurrent client write that bumps DB version mid-sweep | Run sweep               | Sweep write fails for that row with version mismatch; row left at concurrent client's version (next sweep run picks it up) |
| 408 | Chain disable after sweep completes       | Run sweep to completion                                 | Inspect chain state     | Chain is disabled; subsequent reads bypass mutators                                                                        |

### 8.6 Suite F — Live Traffic Under Migration

Validates the actual ZDU promise.

| TC  | Name                                      | Setup                                                          | Concurrent action                    | Assertion                                                                                                        |
| --- | ----------------------------------------- | -------------------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| 501 | Reads return new format mid-sweep         | 1000 v1 entities; sweep batch=50, delay=500 ms                 | Continuous reads at 50 RPS           | All reads return v2 shape; no NPE; latency p99 within 2× baseline                                                |
| 502 | Writes persist as new format mid-sweep    | Same setup                                                     | Continuous writes (UPSERT) at 10 RPS | Each write persists as v2 in DB regardless of sweep state                                                        |
| 503 | Read consistency across sweep boundary    | Read URN A, sweep migrates A, read A again                     | Sequential reads                     | Both reads return same v2 shape; no spurious differences                                                         |
| 504 | Sweep + concurrent writes don't lose data | Sweep processing URN X, concurrent write to X with new content | Read X after sweep                   | Final state = concurrent write content; sweep silently rejected via IF_VERSION_MATCH; sweep retries on next pass |
| 505 | ES dual-write under live load             | Phase 2 catch-up running while clients write 100 entities      | Watch ES indices                     | All 100 present in BOTH old (rollback) and new (primary) indices; doc counts match                               |
| 506 | Read sees catch-up in progress            | Phase 2 emitting RESTATE MCLs for catch-up window              | Read entity in catch-up batch        | Read returns latest data; no 5xx                                                                                 |
| 507 | Ingestion run during ZDU                  | Start a metadata ingestion job, then upgrade mid-run           | Validate ingestion (`csv-enricher`)  | Job either completes or fails cleanly with retryable error; no half-written entities                             |

### 8.7 Suite G — Rollback

| TC  | Name                                      | Setup                                                                                        | Action                      | Assertion                                                                                               |
| --- | ----------------------------------------- | -------------------------------------------------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------- |
| 601 | Roll back to old GMS reads from old index | Phase 1 done; alias swapped to `next`; dual-write active; downgrade GMS image to old version | Read entity through old GMS | Read succeeds; data fetched from `oldPhysicalIndex` directly (old code doesn't know the new index name) |
| 602 | Roll back loses new-only writes           | After 601, write entity ONLY to new index (bypass dual-write)                                | Read via old GMS            | Entity not found (acceptable per spec — rollback may lose writes)                                       |
| 603 | Re-roll-forward after rollback            | After 601, re-deploy new GMS                                                                 | Read entity                 | Reads return v2; system functional                                                                      |
| 604 | Rollback with sweep partially complete    | Half the entities migrated to v2                                                             | Roll back to old GMS        | Old GMS reads v1 entities natively; v2 entities cause NPE/error (expected; logged)                      |

### 8.8 Suite H — Failure Recovery

| TC  | Name                                   | Setup                           | Action        | Assertion                                                                        |
| --- | -------------------------------------- | ------------------------------- | ------------- | -------------------------------------------------------------------------------- |
| 701 | Phase 1 ES connection lost mid-reindex | Inject ES network failure       | Run blocking  | Job fails; `next` index deleted; can re-run cleanly                              |
| 702 | MAE consumer crash during catch-up     | OOM-kill MAE mid-RESTATE batch  | Restart MAE   | Catch-up job not affected (it just emits MCLs); MAE catches up from Kafka offset |
| 703 | DB unavailable during sweep            | Stop MySQL container mid-sweep  | Restart MySQL | Sweep retries (or fails cleanly); cursor preserved; resume on next run           |
| 704 | Schema registry unavailable            | Stop schema-registry mid-job    | Restart it    | Upgrade job retries Kafka writes; eventually succeeds                            |
| 705 | Two upgrade jobs run concurrently      | Start two `system-update-debug` | Wait          | Second one fails fast or no-ops (`DataHubUpgradeResult` acts as lock)            |

### 8.9 Suite I — K8s Scale-Down (Legacy Path) — Optional

Only runnable when KIND or minikube is available. Lower priority. Deferred until at least one
production ZDU has happened on AWS/EKS to inform what's actually needed.

| TC  | Name                                        | Assertion                                                                     |
| --- | ------------------------------------------- | ----------------------------------------------------------------------------- |
| 801 | Scale-down captures replicas to ConfigMap   | ConfigMap `*-system-update-scale-down-state` has `attempt=1`, deployment list |
| 802 | Scale-down deletes KEDA ScaledObjects       | `kubectl get scaledobject` empty for configured deployments                   |
| 803 | Scale-down restores on max-retry exhaustion | After (`maxRetries+1`) failures, replicas restored to original                |
| 804 | ZDU path skips scale-down entirely          | `incrementalReindexEnabled=true` → ConfigMap absent                           |

---

## 9. Test Scenario Sheet

The scenario sheet (Google Sheets) is the **single source of truth** for test cases. It is
mirrored locally as `smoke-test/tests/zdu/scenarios.csv` for offline runs.

| Column               | Purpose                                                         |
| -------------------- | --------------------------------------------------------------- |
| `TC#`                | Integer test case ID (1–23+)                                    |
| `Test Category`      | Groups tests by concern (Suite A/B/C/...)                       |
| `Name`               | Short human-readable name                                       |
| `Description`        | Full description of what is being tested                        |
| `Prerequisite Steps` | Setup required before running the test steps                    |
| `Test Steps`         | Ordered steps to execute                                        |
| `Expected Result`    | What the framework asserts                                      |
| `Actual Result`      | Written by the framework during execution                       |
| `Result`             | `PASS` / `FAIL` / `XFAIL` / `XPASS` / `SKIP`                    |
| `Details`            | Notes, PR links, known-failure explanations                     |
| `Scenario Type`      | Dispatch key (`aspect_migration`, `es_phase1`, `dual_write`, …) |

Adding a new TC = adding a new row. No code change required for scenarios that fit an existing
executor type.

---

## 10. Cross-Cutting Concerns

### 10.1 Test Isolation

**Problem:** 60+ scenarios sharing one Compose stack will collide on URNs, indices, and DB state.

**Solution — three-tier isolation:**

1. **URN namespacing** — every scenario gets a unique prefix:
   `urn:li:dataset:(urn:li:dataPlatform:mysql,zdu-tc101-{uuid7},PROD)`. Cleanup at scenario
   teardown deletes by prefix.
2. **Per-suite Compose stacks** — each suite (A through I) runs in its own Compose project name
   (`zdu-suite-a`, `zdu-suite-b`, …). Suites can be parallelized in CI.
3. **State reset between scenarios** — `reset_scenario_state()` truncates only the scenario's
   URN range from MySQL, ES, and Kafka topics where applicable. Full Compose teardown only
   between suites.

### 10.2 Determinism

ZDU has inherent timing-sensitive paths (T0 vs T1, sweep batch interleaving). Strategies:

- **Inject delays via env vars** — `SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS` already
  exists. Add `BUILD_INDICES_REINDEX_DELAY_MS` and `CATCH_UP_BATCH_DELAY_MS` to make timing
  windows controllable.
- **Log-driven synchronization** — `LogMonitor` blocks until specific log lines appear. Use this
  pattern uniformly — never `time.sleep(N)` to wait for state.
- **Idempotent assertions** — every assertion retries with exponential backoff (max 30 s) before
  failing, since ES is eventually consistent.

### 10.3 Observability During Test Run

Every test run produces:

- `build/zdu-test-report.json` — pass/fail per scenario, timing, error details.
- `build/zdu-failure-{timestamp}/{scenario}/` on failure — full Compose logs per service,
  ES `_cat/indices`, ES `_cat/aliases`, full `DataHubUpgradeResult` aspect content, MySQL
  aspect rows for the scenario's URN range, reader/writer events JSONL.
- `build/zdu-test-report.html` — human-readable summary rendered from JSON (optional).

### 10.4 Hooks Into Production Code

The framework needs three hooks added to production code (these are the only product-level
changes proposed):

| Hook                                               | Purpose                                                                                       | Risk                                      |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------- |
| `SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS` | Already exists                                                                                | None                                      |
| `BUILD_INDICES_REINDEX_DELAY_MS`                   | Inject configurable delay before alias swap to widen the `[T0, T1]` window for catch-up tests | Test-only flag, default 0, no prod change |
| `ASPECT_MIGRATION_CHAIN_DISABLE_BARRIER`           | Block `chain.disable()` until test signals ready, to test "chain disabled" assertion          | Test-only flag, default off               |

These flags must be guarded by `if (env != "test")` conditions or only readable from a
debug-profile properties file. Will be added in a separate PR with explicit reviewer attention.

### 10.5 Concurrency Model

- One reader thread per pool of test entities (default 3 readers).
- Two writer threads (default), sweep-synchronized via `LogMonitor` `BATCH_URNS` events.
- Writer threads exclusively target IO-pool URNs (`zdu-io-pool-{0..4}`) — never TC scenario URNs
  — so the deterministic race window cannot corrupt edge-case entities (e.g., TC-009 "future v5
  must stay at v5").
- Reader threads observe all entities and record `(urn, schemaVersion, ts)` triples for
  monotonicity assertion in the validation phase.

### 10.6 CI Integration

```
Job: zdu-e2e-tests
  Matrix:
    suite: [A, B, C, D, E, F, G, H]
  Steps:
    - checkout
    - build images (cached)
    - bring up Compose stack (suite-specific project name)
    - run pytest -m "suite_${suite}" with 30 min timeout
    - on failure: upload build/zdu-failure-*/ as artifact
    - tear down Compose stack
```

Suite I (K8s) runs only nightly with KIND.

Estimated wall-time per suite: 5–15 min. Total parallelized: ~30 min.

---

## 11. Operating the Framework

### 11.1 Prerequisites

| Requirement                | Notes                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------- |
| Docker + Docker Compose v2 | Compose plugin (`docker compose`) must be on `$PATH`                                   |
| DataHub running locally    | Start with `scripts/dev/datahub-dev.sh start`                                          |
| Python venv                | `smoke-test/venv/bin/python` (created by Gradle or `scripts/dev/datahub-dev.sh setup`) |
| GMS token                  | Run `datahub init --username datahub --password datahub` once                          |

### 11.2 Quick Start

```bash
# Full pytest run (all suites, all TCs)
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN=<token> \
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py -v

# CLI entry point (no pytest, just exit code)
smoke-test/venv/bin/python -m tests.zdu \
  --gms-url http://localhost:8080 \
  --gms-token <token>

# AGENT_MODE for machine-readable JSON
AGENT_MODE=1 scripts/dev/datahub-dev.sh test smoke-test/tests/zdu/
```

### 11.3 Configuration

All env variables read by `ZDUTestConfig.from_env()` at startup.

| Variable                 | Default                 | Description                                                                                                                              |
| ------------------------ | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_GMS_URL`        | `http://localhost:8080` | GMS base URL                                                                                                                             |
| `DATAHUB_GMS_TOKEN`      | _(none)_                | Bearer token for authenticated instances                                                                                                 |
| `ZDU_SWEEP_TIMEOUT`      | `600`                   | Seconds to wait for the sweep to complete                                                                                                |
| `ZDU_SKIP_PHASES`        | _(none)_                | Comma-separated phase names to skip                                                                                                      |
| `ZDU_READER_WORKERS`     | `3`                     | Concurrent reader threads during sweep                                                                                                   |
| `ZDU_WRITER_WORKERS`     | `2`                     | Concurrent writer threads during sweep                                                                                                   |
| `ZDU_PRE_WRITE_DELAY_MS` | `500`                   | Sets `SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS` in the upgrade container — controls deterministic race window. Set 0 to disable. |
| `ZDU_PROJECT_DIR`        | `docker/profiles/`      | Docker Compose project directory                                                                                                         |
| `ZDU_GMS_SERVICE`        | `datahub-gms-debug`     | Compose service name for GMS                                                                                                             |
| `ZDU_UPGRADE_SERVICE`    | `system-update-debug`   | Compose service name for the upgrade job                                                                                                 |
| `ZDU_REBUILD_CWD`        | _(repo root)_           | Working directory for `scripts/dev/datahub-dev.sh rebuild --wait`                                                                        |
| `ZDU_REINDEX_DELAY_MS`   | `0`                     | (Suite B/D) Sets `BUILD_INDICES_REINDEX_DELAY_MS` to widen the `[T0, T1]` window predictably.                                            |
| `ZDU_CATCH_UP_DELAY_MS`  | `0`                     | (Suite D) Sets `CATCH_UP_BATCH_DELAY_MS` for catch-up batch interleave.                                                                  |

### 11.4 CLI Arguments

| Argument           | Default                 | Description                                    |
| ------------------ | ----------------------- | ---------------------------------------------- |
| `--gms-url`        | `http://localhost:8080` | GMS base URL                                   |
| `--gms-token`      | _(none)_                | Bearer token                                   |
| `--skip PHASE …`   | _(none)_                | One or more phase names to skip                |
| `--sweep-timeout`  | `600`                   | Seconds before sweep is considered timed out   |
| `--reader-workers` | `3`                     | Concurrent readers during sweep                |
| `--writer-workers` | `2`                     | Concurrent writers during sweep                |
| `--only-tc TC …`   | _(all)_                 | Run only the listed TC numbers                 |
| `--suite NAME`     | _(all)_                 | Run only the listed suites (a/b/c/d/e/f/g/h/i) |

### 11.5 Phase Skipping

```bash
# Skip the upgrade and sweep — validate only (assumes a prior run already upgraded)
ZDU_SKIP_PHASES=upgrade_blocking,upgrade_nonblocking \
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/test_zdu_upgrade.py -v

# CLI equivalent
python -m tests.zdu --skip upgrade_blocking upgrade_nonblocking
```

Available phase names: `discovery`, `seed`, `snapshot_t0`, `upgrade_blocking`,
`inject_traffic_pre`, `rolling_restart`, `inject_traffic_dual`, `upgrade_nonblocking`,
`runtime_migration`, `validation`.

### 11.6 Filtering Test Cases

```bash
# Run only TC-001 through TC-006
python -m tests.zdu --only-tc 1 2 3 4 5 6

# Pytest -k filter
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py \
  -k "TC-001 or TC-004 or TC-007" -v

# Single suite
python -m tests.zdu --suite b
```

### 11.7 Result Statuses

| Status  | Meaning                                                                        |
| ------- | ------------------------------------------------------------------------------ |
| `PASS`  | Entity reached the expected `schemaVersion` (or assertion held)                |
| `FAIL`  | Wrong version / assertion failed; see `failure_reason`                         |
| `XFAIL` | Known failure — scenario is listed in `KNOWN_FAILURES` in `scenario_loader.py` |
| `XPASS` | Scenario expected to fail but passed — actionable; consider removing XFAIL     |
| `SKIP`  | Scenario has no seeded entity (e.g., TC-023 rolling upgrade)                   |

### 11.8 Report File

`smoke-test/build/zdu-test-report.json` after every run:

```json
{
  "config": { "gms_url": "...", "sweep_timeout_s": 600 },
  "phase_results": [
    {
      "phase_name": "discovery",
      "status": "passed",
      "duration_s": 0.7,
      "details": {}
    }
  ],
  "scenario_results": [
    { "tc_number": 1, "name": "Full sweep single hop", "status": "PASS" }
  ],
  "started_at": "...",
  "ended_at": "..."
}
```

### 11.9 Troubleshooting

**Sweep times out (600 s)** — the upgrade job container requires
`DATAHUB_TOKEN_SERVICE_SIGNING_KEY` and `DATAHUB_TOKEN_SERVICE_SALT`. The framework reads these
from the running GMS container automatically via `docker inspect`. If GMS is not running, the
upgrade job fails to start. Ensure DataHub is up before running tests.

**`ASPECT_MIGRATION_MUTATOR_ENABLED=false` warning after upgrade** — the rebuild did not include
the flag. Set it before rebuilding:

```bash
scripts/dev/datahub-dev.sh env set ASPECT_MIGRATION_MUTATOR_ENABLED=true
scripts/dev/datahub-dev.sh env restart
```

**Sweep emits SKIPPED (prior SUCCEEDED result)** — the framework auto-detects this, deletes the
prior `urn:li:dataHubUpgrade:migrate-aspects-<version>` URN, and retries with a new
timestamp-based version. If it still skips, check the DB for stale upgrade URNs.

**401 Unauthorized on seed** — run `datahub init --username datahub --password datahub` and set
`DATAHUB_GMS_TOKEN` to the returned token.

**Rebuild fails with `rc=127`** — the rebuild command runs from the repo root by default
(`ZDU_REBUILD_CWD`). Ensure you are not overriding it to a subdirectory.

---

## 12. Implementation Status & Roadmap

### Phase 1 — Suite A (DONE)

`smoke-test/tests/zdu/` on branch `zdu-test-framework-master`:

- 5-phase pipeline (legacy form).
- 23 scenarios.
- Sweep-synchronized concurrent IO.
- JSON report.

### Phase 2 — ES Infrastructure (4–6 weeks)

1. `ElasticsearchClient` extensions + new `MySQLClient`.
2. `snapshot_t0` and `upgrade_blocking` phases.
3. Suite B (TC-101 to TC-112).
4. Suite D (TC-301 to TC-309).

### Phase 3 — Dual-Write & Live Traffic (3–4 weeks)

5. `inject_traffic_pre`, `inject_traffic_dual`, `inject_traffic_post` phases.
6. `rolling_restart` phase.
7. Suite C (TC-201 to TC-208).
8. Suite F (TC-501 to TC-507).

### Phase 4 — Failure & Rollback (3 weeks)

9. Suite G (TC-601 to TC-604).
10. Suite H (TC-701 to TC-705).
11. Failure-artifact collection in `Reporter`.

### Phase 5 — System-Level Sweep Coverage (1–2 weeks)

12. Suite E (TC-401 to TC-408) — extends Suite A scenarios.

### Phase 6 — K8s (Deferred / Optional, 4 weeks)

13. KIND harness.
14. Suite I (TC-801 to TC-804).

---

## 13. Future Extensions

| #   | Extension                        | Notes                                                                                                                |
| --- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| 1   | **Additional scenario coverage** | Any new aspect migration only requires a new row in the scenario sheet — no code change.                             |
| 2   | **Stress mode**                  | Configurable entity count per scenario scaled to thousands; tests sweep throughput and cursor resumption under load. |
| 3   | **HTML report**                  | Render `zdu-test-report.json` to a human-readable HTML summary with timing graphs.                                   |
| 4   | **K8s harness (Suite I)**        | KIND-based runner mirroring Helm pre-hook + rolling restart + post-hook semantics.                                   |
| 5   | **Cross-version matrix**         | Run the framework against multiple `from → to` version pairs to catch multi-hop schema regressions.                  |

---

## 14. Success Criteria

The framework is "done" when **every phase in Section 5 is exercised end-to-end against a real
Compose stack** and validates the invariants below. Each phase has its own checklist —
nothing is taken on faith; every component listed under "Touches / Simulates" must be hit by at
least one scenario, and every "Must verify" assertion must be encoded as a programmatic check.

### 14.1 Per-Phase Success Criteria

#### Phase 1 — Discovery

- **Touches / Simulates**
  - Docker Compose service inventory (`docker compose ps`).
  - GMS `/health` endpoint.
  - Elasticsearch `_cluster/health`.
  - MySQL `SELECT 1`.
  - Kafka broker reachability via `AdminClient.describeCluster()`.
- **Must verify**
  - [ ] Image digest captured for `datahub-gms`, `datahub-mae-consumer`, `datahub-mce-consumer`,
        `datahub-frontend-react`, `system-update-debug`.
  - [ ] GMS returns 200 within timeout; otherwise the runner aborts before seeding.
  - [ ] ES cluster status is `green` or `yellow` (not `red`).
  - [ ] MySQL connection acquired via `MySQLClient`.
  - [ ] Kafka connection acquired and core topics (`MetadataChangeProposal_v1`,
        `MetadataChangeLog_Versioned_v1`, `MetadataChangeLog_Timeseries_v1`) exist.
  - [ ] Discovery output is recorded in `TestContext.discovery` for later phases to diff against.

#### Phase 2 — Seed

- **Touches / Simulates**
  - GMS write path (REST `ingestProposal`).
  - Direct MySQL `INSERT INTO metadata_aspect_v2` (IO-pool entities, bypassing mutators).
  - `ASPECT_MIGRATION_MUTATOR_ENABLED=false` precondition.
- **Must verify**
  - [ ] Each scenario's expected entity exists in MySQL at the declared starting state
        (`null` / v1 / v2 / v4 / v5).
  - [ ] IO-pool entities (`zdu-io-pool-{0..4}`) land at `schemaVersion=null` regardless of
        feature flag (proves direct-DB seed is mutator-bypassing).
  - [ ] No spurious `schemaVersion` upgrades during seed (warn-and-continue if the flag is
        already on).
  - [ ] Per-entity data-shape validator function is registered in `TestContext.scenarios`.
  - [ ] Seed report contains URN, aspect name, starting `schemaVersion`, expected post-migration
        `schemaVersion` for every seeded row.

#### Phase 3 — Snapshot T0

- **Touches / Simulates**
  - ES `_cat/indices`, `_cat/aliases`, per-index `_count`.
  - MySQL `SELECT COUNT(*) ... GROUP BY systemmetadata->'$.schemaVersion'`.
  - `DataHubUpgradeResult` aspect read.
  - System wall-clock at `now()` for T0 capture.
- **Must verify**
  - [ ] Pre-upgrade physical index names recorded for every alias used by Suites B/D.
  - [ ] Per-index doc counts captured and stored in `TestContext.snapshot_t0`.
  - [ ] Per-`schemaVersion` aspect counts captured for every aspect under test.
  - [ ] T0 epoch (millis) recorded.
  - [ ] `DataHubUpgradeResult` is absent OR in a terminal state for the upcoming version
        (no in-flight upgrade for the same version exists).

#### Phase 4 — Upgrade Blocking

- **Touches / Simulates**
  - `system-update-debug` container running `-u SystemUpdateBlocking`.
  - `BuildIndicesIncrementalStep` execution.
  - ES `_reindex` task creation, polling, and completion.
  - ES alias swap (`POST /_aliases`).
  - Kafka topic creation/migration if applicable.
  - `DataHubUpgradeResult` aspect write back to MySQL.
- **Must verify**
  - [ ] One log line `Reindexing index '{x}' -> '{next}' with task {taskId}` per affected index.
  - [ ] Polling logs observed: `Polling task {taskId}: status=..., completed=N/M` until completion.
  - [ ] Exactly one `Alias swapped: {alias} -> {nextIndex}` per affected alias.
  - [ ] `DataHubUpgradeResult.indicesState[{name}].status = COMPLETED` for every reindexed index.
  - [ ] Required keys present per index: `nextIndexName`, `oldBackingIndexName`,
        `reindexStartTime`, `sourceDocCount`, `taskId`, `requiresDataBackfill`.
  - [ ] Old physical index still exists after alias swap (rollback safety).
  - [ ] Re-running the blocking job emits `already COMPLETED in previous run, skipping` (TC-109).
  - [ ] Interrupted job resumes via `Resuming polling for index ...` line (TC-110).

#### Phase 5 — Inject Traffic Pre

- **Touches / Simulates**
  - Old GMS write path (REST `ingestProposal` against `vN-old` GMS still running).
  - Direct ES query against the **old** physical index by name (bypassing alias).
  - Kafka MCP topic.
  - MySQL aspect insert.
- **Must verify**
  - [ ] At least N writes (default 10) succeed against the old GMS during the window.
  - [ ] Each written entity is queryable in the **old physical index** by direct GET.
  - [ ] Each written entity is **NOT** queryable in the new physical index (the gap entities
        only exist in old until catch-up backfills them).
  - [ ] Each written entity is queryable in MySQL.
  - [ ] T0–T1 gap URN list is recorded in `TestContext.gap_urns` for Phase 8 catch-up
        verification.

#### Phase 6 — Rolling Restart

- **Touches / Simulates**
  - `docker compose up --build -d datahub-gms` (new image).
  - `docker compose up --build -d datahub-mae-consumer` (new image).
  - `docker compose up --build -d datahub-mce-consumer` (new image).
  - `UpdateIndicesUpgradeStrategy` initialization on new MAE.
  - MAE state-poller startup.
- **Must verify**
  - [ ] Restart sequence is deterministic: GMS first, then MAE, then MCE (no parallel restart).
  - [ ] Each restarted service reports healthy (`/health` 200) before the next service restarts.
  - [ ] New MAE log line `Recorded dual-write start time for index '{name}'` observed for every
        affected index.
  - [ ] T1 epoch captured per index from `dualWriteStartTime` in `DataHubUpgradeResult`.
  - [ ] T1 > T0 for every index (otherwise the gap window is invalid).
  - [ ] No traffic during the restart returns 5xx for longer than the configured restart window.

#### Phase 7 — Inject Traffic Dual

- **Touches / Simulates**
  - New GMS write path.
  - MAE consumer applying MCL → ES with dual-write fan-out.
  - Direct ES queries against both old and new physical indices.
- **Must verify**
  - [ ] At least N writes (default 10) succeed against the new GMS during the dual-write window.
  - [ ] Every written entity appears in **both** the old and new physical indices.
  - [ ] Document content matches in both indices (byte-for-byte for stable fields).
  - [ ] When `rollbackDualWriteEnabled=false`, writes appear ONLY in the new index (TC-204).
  - [ ] `DataHubUpgradeResult.indicesState[{name}].dualWriteStartTime` is set exactly once
        per index, matching the timestamp of the first dual-write (TC-205).
  - [ ] Deletes propagate to both indices (TC-208).

#### Phase 8 — Upgrade Non-Blocking

- **Touches / Simulates**
  - `system-update-debug` running `-u SystemUpdateNonBlocking`.
  - `MigrateAspectsStep` (aspect schema sweep through mutator chain).
  - `IncrementalReindexCatchUpStep` (T0–T1 gap backfill).
  - `DUAL_WRITE_DISABLED` marking on indices.
  - `AspectMigrationMutatorChain.disable()` on completion.
  - Concurrent reader threads, writer threads, and IO-pool race-window.
  - `ConditionalWriteValidator` + `IF_VERSION_MATCH` header on sweep writes.
- **Must verify (Sweep)**
  - [ ] `Migration sweep starting` observed in upgrade-job logs.
  - [ ] One `Processing batch URNs: ...` line per batch; URN list parsed and forwarded to
        writer threads via `LogMonitor`.
  - [ ] Total entities migrated equals `sourceDocCount - alreadyMigrated`.
  - [ ] `Sweep complete, total {n}` observed.
  - [ ] `DataHubUpgradeResult` for the sweep step transitions through `STARTED` → `COMPLETED`.
  - [ ] APP_SOURCE = `SYSTEM_UPDATE` stamped on `systemMetadata.properties` of every sweep-written
        aspect (TC-406).
  - [ ] Cursor resumability: a killed sweep restarts at `Resuming from createdOn >= {ms}` and
        does not double-migrate any row (TC-401).
- **Must verify (Catch-up)**
  - [ ] One `Catch-up for entity index {name}: window [{T0}, {T1}]` per index.
  - [ ] Every URN recorded in `TestContext.gap_urns` (Phase 5) is present in the new physical
        index after catch-up.
  - [ ] Filtered `_reindex` task is observed for timeseries indices (TC-302).
  - [ ] Catch-up resume line `Resuming catch-up for index {name} from URN: {urn}` observed
        when interrupted (TC-307).
- **Must verify (Dual-write disable)**
  - [ ] When `rollbackDualWriteEnabled=false`, every COMPLETED index is marked
        `DUAL_WRITE_DISABLED` (TC-308).
  - [ ] When `rollbackDualWriteEnabled=true`, status remains COMPLETED until operator
        intervention (TC-309).
  - [ ] MAE state poller logs `Removed rollback dual-write target for entity '{name}'`
        within one polling interval (TC-206).
  - [ ] Eventually `All dual-write targets removed, shutting down state poller` (TC-207).
- **Must verify (Race window)**
  - [ ] For every IO-pool URN that received a concurrent client write during the sweep, the
        final stored `schemaVersion` matches the client write — not the sweep write.
  - [ ] At least one `ConditionalWriteValidator` rejection observed in GMS logs for the
        IO-pool URNs (proves the race window actually fired).
  - [ ] `[upgrade] sweep URN: <X>` and `[gms] Producing MCL ...` lines for the same URN
        appear in the test output, proving line-by-line interleaving.

#### Phase 9 — Runtime Aspect Read and Write (Mutator Chain)

- **Touches / Simulates**
  - GMS read path with `AspectMigrationReadMutator`.
  - GMS write path with `AspectMigrationWriteMutator`.
  - Live entities at mixed `schemaVersion` (some pre-sweep, some post-sweep).
  - `chain.disable()` post-sweep behavior.
- **Must verify**
  - [ ] Reading a pre-ZDU row (`schemaVersion=null` or v1) returns the upgraded shape
        through the API even though the DB row may still be stale.
  - [ ] Writing through the new GMS persists rows directly at the latest `schemaVersion`
        with the new shape (no sweep needed for writes).
  - [ ] Read progression invariant: across all reader-thread observations, no URN ever moves
        backward in `schemaVersion` (`(urn, schemaVersion, ts)` tuples are monotonic).
  - [ ] After sweep completes and `AspectMigrationMutatorChain disabled` is logged, subsequent
        reads of fully-migrated rows bypass mutators (TC-019 / TC-408).
  - [ ] Read latency p99 stays within 2× baseline during the sweep (TC-501).
  - [ ] Write throughput holds at 10 RPS without 5xx during sweep (TC-502).

#### Phase 10 — Validation

- **Touches / Simulates**
  - MySQL aspect rows for every seeded URN.
  - ES indices and aliases via direct queries.
  - `DataHubUpgradeResult` aspect.
  - Reader/writer thread observation logs from Phase 8 / 9.
- **Must verify (5 dimensions)**
  - [ ] **Per-entity schema version** — every seeded URN's stored `schemaVersion` matches the
        scenario's expected value; data shape passes the per-scenario validator function.
  - [ ] **Write path** — every entity written by a writer thread during the sweep has the
        latest `schemaVersion` on the very first stored copy (no sweep-rewrite needed).
  - [ ] **Read progression** — reader observations grouped by URN are monotonic in
        `schemaVersion` over time.
  - [ ] **Elasticsearch field presence** — for ES-bearing scenarios, the search index contains
        the migrated fields; the index mapping reflects the new shape.
  - [ ] **Alias targets and dual-write state** — every alias points at the expected physical
        index; per-index `dualWriteStartTime` matches expectation; indices are marked
        `DUAL_WRITE_DISABLED` if and only if the rollback flag is off.
  - [ ] Validation phase collects ALL failures (does not fail-fast); failure block contains
        URN, expected, got, and reason for every failure.
  - [ ] Failure artifact bundle is written to `build/zdu-failure-{timestamp}/{scenario}/` with
        compose-logs, ES cat outputs, upgrade result JSON, MySQL CSV, and reader/writer events.

### 14.2 Suite-Level Success Criteria

- [ ] **Suite A** — all 23 TCs execute; XFAIL set matches the documented known-failure list;
      no XPASS in steady state.
- [ ] **Suite B** — every TC in 101–112 produces an explicit pass/fail (no skips outside
      `incrementalReindexEnabled=false`).
- [ ] **Suite C** — TC-201 to TC-208 all pass with both `rollbackDualWriteEnabled` true and
      false matrices.
- [ ] **Suite D** — TC-301 to TC-309 all pass; gap URNs from Phase 5 are 100% covered by
      catch-up.
- [ ] **Suite E** — TC-401 to TC-408 pass; cursor resumability validated by an explicit kill+
      restart in CI.
- [ ] **Suite F** — TC-501 to TC-507 pass under load; no read returns stale shape, no write
      lost, p99 read latency ≤ 2× baseline.
- [ ] **Suite G** — rollback to old GMS preserves dual-written data; TC-602 confirms
      acceptable loss boundary; TC-603 re-roll-forward succeeds.
- [ ] **Suite H** — every failure injection (ES partition, MAE OOM, DB stop, schema-registry
      stop, double-upgrade lock) recovers cleanly with cursor preservation.
- [ ] **Suite I** _(optional / deferred)_ — KIND harness exists and TC-801 to TC-804 run
      nightly when enabled.

### 14.3 Framework-Level Success Criteria

- [ ] CI runs the full Suite A–H matrix on every PR to `master` that touches `metadata-io/`,
      `entity-registry/`, `datahub-upgrade/`, or `metadata-models/`.
- [ ] CI total wall-time ≤ 30 minutes with per-suite parallelism.
- [ ] Every phase produces a structured `PhaseResult` with status, duration, and details.
- [ ] Every test run produces `build/zdu-test-report.json` matching the documented schema.
- [ ] Every failure produces an artifact bundle that lets a reviewer diagnose root cause in
      ≤ 5 minutes.
- [ ] Every phase has at least one unit test (`framework/test_*.py`) covering its parser /
      core helper.
- [ ] Adding a new TC to Suites A or E requires only a new row in the Google Sheet (no code
      change).
- [ ] A new ZDU contributor can write a new scenario in < 30 minutes using existing patterns.
- [ ] Mean-time-to-discovery for ZDU regressions drops to < 1 day (vs. current
      "discovered in production").

### 14.4 Phase × Track Coverage Matrix

Every cell that says "✓" must be exercised by at least one passing scenario. Empty cells are
intentional (the track does not run in that phase).

| Phase                  | Aspect schema | ES Phase 1 | ES dual-write | ES Phase 2 catch-up | Mutator chain |
| ---------------------- | :-----------: | :--------: | :-----------: | :-----------------: | :-----------: |
| 1 Discovery            |       ✓       |     ✓      |               |                     |               |
| 2 Seed                 |       ✓       |     ✓      |               |                     |       ✓       |
| 3 Snapshot T0          |       ✓       |     ✓      |       ✓       |          ✓          |               |
| 4 Upgrade Blocking     |               |     ✓      |               |                     |               |
| 5 Inject Traffic Pre   |               |     ✓      |               |          ✓          |               |
| 6 Rolling Restart      |               |            |       ✓       |                     |               |
| 7 Inject Traffic Dual  |               |            |       ✓       |                     |               |
| 8 Upgrade Non-Blocking |       ✓       |            |       ✓       |          ✓          |       ✓       |
| 9 Runtime Read/Write   |       ✓       |            |               |                     |       ✓       |
| 10 Validation          |       ✓       |     ✓      |       ✓       |          ✓          |       ✓       |

A track is "covered" when every ✓ cell has at least one TC verifying it; a phase is "complete"
when every ✓ cell in its row has at least one TC verifying it.

---

## 15. Glossary

| Term                            | Meaning                                                                                                                 |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| ZDU                             | Zero-Downtime Upgrade — DataHub's umbrella for safe rolling upgrades.                                                   |
| Phase 1 (blocking)              | Pre-restart upgrade work: ES reindex into a `next` index, alias swap. Must finish before new pods start.                |
| Phase 2 (non-blocking)          | Post-restart work: aspect schema sweep, ES catch-up, optional dual-write disable. Runs while traffic flows.             |
| Dual-write                      | After alias swap, the new GMS writes each index update to both old and new physical indices for rollback safety.        |
| `[T0, T1]` window               | T0 = ES reindex start time; T1 = dual-write start time on the new GMS. Writes in this gap landed only in the old index. |
| Rollback flag                   | `rollbackDualWriteEnabled` — controls whether catch-up automatically marks indices as `DUAL_WRITE_DISABLED`.            |
| IO pool                         | Test-only entities (`zdu-io-pool-{0..4}`) seeded directly into MySQL to keep them at v1; targets for concurrent writes. |
| Mutator chain                   | `AspectMigrationMutatorChain` — registered v*n → v*{n+1} transformers applied on read, write, and sweep.                |
| `bridgeGap()`                   | Mechanism to advance `schemaVersion` past a missing intermediate mutator.                                               |
| `IF_VERSION_MATCH`              | Conditional write header: sweep writes use it to avoid stomping concurrent client writes.                               |
| MAE                             | Metadata Audit Event consumer — processes MCLs and updates ES indices.                                                  |
| MCE                             | Metadata Change Event consumer — accepts MCPs and writes to MySQL/Kafka.                                                |
| MCL / MCP                       | Metadata Change Log / Metadata Change Proposal.                                                                         |
| `DataHubUpgradeResult`          | Aspect on `urn:li:dataHubUpgrade:...` — control plane recording per-step state, doc counts, alias targets, T0, T1.      |
| `BuildIndicesIncrementalStep`   | Phase 1 step: reindex into `next` index and swap alias.                                                                 |
| `IncrementalReindexCatchUpStep` | Phase 2 step: backfill `[T0, T1]` gap, optionally mark indices `DUAL_WRITE_DISABLED`.                                   |
| `MigrateAspectsStep`            | Phase 2 step: read every target aspect from DB and re-write via the mutator chain.                                      |
| `UpdateIndicesUpgradeStrategy`  | MAE-side dual-write logic that fans writes to old + new physical indices during the rollback window.                    |
