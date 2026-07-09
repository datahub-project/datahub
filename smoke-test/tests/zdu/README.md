# ZDU (Zero-Downtime Upgrade) Integration Test Framework

End-to-end integration tests for DataHub's aspect schema migration system. The framework seeds old-format entities into a live DataHub instance, performs a rolling upgrade, triggers the background sweep, and validates that every entity lands at the expected `schemaVersion` — while concurrently reading and writing to verify the migration path handles live traffic correctly.

---

## Prerequisites

| Requirement                | Notes                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------- |
| Docker + Docker Compose v5 | Compose plugin (`docker compose`) must be on `$PATH`                                   |
| DataHub running locally    | Start with `scripts/dev/datahub-dev.sh start`                                          |
| Python venv                | `smoke-test/venv/bin/python` (created by Gradle or `scripts/dev/datahub-dev.sh setup`) |
| GMS token                  | Run `datahub init --username datahub --password datahub` once                          |

The smoke-test venv is managed by Gradle. Activate it manually when running pytest directly:

```bash
source smoke-test/venv/bin/activate
```

---

## Quick Start

There are **two** equivalent entry points — both construct the same `ZDUTestRunner` and run the same pipeline. They differ only in how results are surfaced:

| Entry point                            | When to use                                                                               | Result format                                                                            |
| -------------------------------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `python -m tests.zdu` (recommended)    | CLI / CI / scripted runs                                                                  | Printed report + JSON at `smoke-test/build/zdu-test-report.json` + non-zero exit on FAIL |
| `pytest tests/zdu/test_zdu_upgrade.py` | When you want per-phase + per-scenario pytest output (xfail/skip native, IDE integration) | pytest UI; same JSON report                                                              |

Running both in the same invocation is not supported — they each build their own `ZDUTestRunner`. Pick one.

**CLI entry (the canonical path):**

```bash
cd "$(git rev-parse --show-toplevel)"  # cd to the datahub repo root
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN=<token> \
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env \
smoke-test/venv/bin/python -m tests.zdu
```

**Pytest entry (per-test reporting):**

```bash
cd "$(git rev-parse --show-toplevel)"
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN=<token> \
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env \
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py -v
```

---

## Two-Image-Tag Testing (Plan F-3)

The framework supports running the upgrade job with a different image tag than what's currently in the stack. This lets you simulate a real ZDU upgrade flow: live GMS/MAE/MCE stay on the OLD image while `Phase 4 (UpgradeBlockingPhase)` runs the upgrade job with the NEW image.

### Prerequisites

Both image tags must already exist locally (or be pullable from your registry):

```bash
# Build the OLD image (whatever release you're upgrading from)
./gradlew :metadata-service:war:docker -Ptag=v1.5.0

# Build the NEW image (what you're upgrading to — usually HEAD)
./gradlew :metadata-service:war:docker -Ptag=v1.6.0
```

(Repeat for `:datahub-upgrade:docker`, etc., for each ZDU-relevant service.)

### Running

```bash
# Bring up the stack with the OLD image
DATAHUB_VERSION=v1.5.0 scripts/dev/datahub-dev.sh start

# Run ZDU tests with explicit OLD/NEW tags
ZDU_OLD_IMAGE_TAG=v1.5.0 \
ZDU_NEW_IMAGE_TAG=v1.6.0 \
DATAHUB_GMS_TOKEN="$(grep '  token:' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

Behaviour:

- **Discovery phase** records both tags in the JSON report's `phases[0].details.{old_image_tag,new_image_tag}`.
- **Phase 4 (upgrade_blocking)** launches `system-update-debug` with `DATAHUB_UPDATE_VERSION=$ZDU_NEW_IMAGE_TAG` so the upgrade job runs the NEW image's logic while the live GMS/MAE/MCE stay on OLD.
- **Phase 6 (rolling_restart, future)** will sequentially swap GMS → MAE → MCE from OLD to NEW.

### Defaults

If neither env var is set, both default to `debug` — matching the existing single-image dev workflow. F-3 is a no-op against a stack started without these env vars.

### Phase 0 + 0.5: Build Images + Prepare OLD Stack (default-on)

`BuildImagesPhase` (Phase 0) and `PrepareOldStackPhase` (Phase 0.5) together provide a one-button workflow: every run deterministically builds OLD/NEW images from git refs **and** redeploys the running Compose stack onto OLD before discovery/seed. No manual `docker pull`, `./gradlew :*:docker`, or stack restarts needed.

**Phase 0 — BuildImagesPhase:**

- **OLD image set** — built from a **persistent** `git worktree` at `smoke-test/build/zdu-images/old/` checked out at the configured OLD ref (default `master`). On rerun, the worktree is synced to the ref's resolved commit via `git fetch + reset --hard` rather than re-created. Tag: `zdu-old-{sha8}` where `sha8` is the 8-char prefix of the resolved commit SHA.
- **NEW image set** — built from a persistent worktree at `smoke-test/build/zdu-images/new/` checked out at the current branch's HEAD. Tag: `zdu-new-{sha8}`.
- **Cached by SHA** — if all required images already exist locally with the computed tags, the phase is a near-instant no-op. Reruns are fast.
- **Services built** — `:metadata-service:war:docker`, `:datahub-upgrade:docker`, `:metadata-jobs:mae-consumer-job:docker`, `:metadata-jobs:mce-consumer-job:docker`. Frontend deferred.
- **Mutates config** — sets `config.old_image_tag` and `config.new_image_tag` before downstream phases construct. `ZDU_OLD_IMAGE_TAG` / `ZDU_NEW_IMAGE_TAG` env vars are overridden by this phase when it runs.

**Phase 0.5 — PrepareOldStackPhase:**

- Reads `config.old_image_tag` (just mutated by Phase 0) and inspects the currently-running images on `datahub-gms-debug`, `datahub-mae-consumer-debug`, `datahub-mce-consumer-debug` via `docker inspect`.
- For each service whose running image **does not** match the OLD tag, recreates that service with `DATAHUB_VERSION` + per-service `DATAHUB_*_VERSION` set to the OLD tag — equivalent to a rolling-back redeploy.
- Polls `${gms_url}/health` until 200 (timeout 180s). On timeout the phase reports `failed` and aborts the run (fail_fast).
- Skipped automatically when `config.old_image_tag == "debug"` — i.e., Phase 0 didn't run, so there is no specific OLD tag to enforce.

**Mandatory by default** — both phases run on every test invocation. To opt out:

- `ZDU_SKIP_BUILD_IMAGES=1` — skip Phase 0 (use existing manually-set or default `debug` tags).
- `ZDU_SKIP_PREPARE_OLD_STACK=1` — skip Phase 0.5 (assumes the stack is already on the OLD image).

**Override OLD ref:** `ZDU_OLD_REF=v1.5.0` to build OLD from a release tag instead of `master`.

**Override worktree root:** `ZDU_BUILD_IMAGES_ROOT=path/to/dir` (default `smoke-test/build/zdu-images`).

**Cold build time** is ~20–35 min per side (40–70 min total). Subsequent runs cache-hit and complete in <2s. Phase 0.5 takes ~1s when the stack is already on OLD; ~60–120s when services need recreation.

```bash
# Default — Phase 0 builds, Phase 0.5 redeploys onto OLD
DATAHUB_GMS_TOKEN="$(grep '  token:' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Build OLD from a specific release tag
ZDU_OLD_REF=v1.5.0 \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Iterating on Python-only changes — skip both phases, reuse existing stack
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

### Phase 5: Inject Traffic Pre

`InjectTrafficPrePhase` fires N writes (default 10) via the running OLD GMS in the gap window between Phase 4 (alias swap) and Phase 6 (rolling restart). Each write becomes a "T0–T1 gap entity" that should land in the OLD physical index only — Phase 8 catch-up (future plan) is responsible for backfilling these into the NEW index.

The phase verifies:

- Each write succeeded against the OLD GMS (REST `ingestProposal`).
- Each entity is queryable in MySQL (`metadata_aspect_v2`).
- (Best-effort, when distinct OLD/NEW physical indices exist) Each entity appears in the OLD index but NOT the NEW index.

Index-distinction checks rely on `ctx.upgrade_blocking.indices[*]` having distinct `old_backing_index_name` and `next_index_name` — when they match (degenerate single-image dev workflow), the phase reduces to write + MySQL verification.

The gap URN list is recorded in `ctx.gap_urns` (also surfaced in `phases[*].details.gap_urns` of the JSON report). Phase 8 catch-up will read this to verify backfill.

### Phase 6: Rolling Restart

`RollingRestartPhase` swaps the running GMS, MAE, and MCE containers from `OLD_IMAGE_TAG` to `NEW_IMAGE_TAG` in sequence. For each service it sets the per-service compose env (`DATAHUB_GMS_VERSION`, `DATAHUB_MAE_VERSION`, `DATAHUB_MCE_VERSION`) plus the global `DATAHUB_VERSION` fallback, runs `docker compose up -d {service}`, and waits for healthy.

After all services restart, the phase tails the MAE log for the line `Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}` and records each index's T1 epoch in `ctx.rolling_restart.dual_write_start_times`. This is the T1 in the `[T0, T1]` window that Phase 7 (InjectTrafficDual, future) and Phase 8 (UpgradeNonBlocking, future) reason about.

If `recreate_service` fails for any service (image-pull error, health timeout), the phase aborts and reports `failed` — subsequent services in the order are NOT restarted, leaving the stack in a mixed state. The operator must investigate before re-running.

### Phase 7: Inject Traffic Dual

`InjectTrafficDualPhase` fires N writes (default 10) via the running NEW GMS (post-rolling-restart) during the dual-write window. Each write should appear in BOTH the OLD physical index (rollback safety net for the 24h post-upgrade window) and the NEW physical index (primary destination).

The phase verifies:

- Each write succeeded against the NEW GMS (REST `ingestProposal`).
- Each entity is queryable in MySQL.
- (Best-effort, when distinct OLD/NEW physical indices exist) Each entity appears in BOTH the OLD and NEW physical indices via direct ES queries.

Index-distinction checks rely on `ctx.upgrade_blocking.indices[*]` having distinct `old_backing_index_name` and `next_index_name`. When they match (degenerate single-image dev workflow), the phase reduces to write + MySQL verification.

The dual-write URN list is recorded in `ctx.dual_write_urns` (also surfaced in `phases[*].details.dual_write_urns` of the JSON report). On hard failure (ingest or MySQL), the partial list of successfully-ingested URNs is preserved.

### Phase 8: Upgrade Non-Blocking

`UpgradeNonBlockingPhase` runs `system-update -u SystemUpdateNonBlocking` against the live NEW GMS while concurrent reader and writer threads continue to hit the system. It replaces the legacy `SweepAndIOPhase` placeholder.

Responsibilities:

- **Aspect schema sweep** — `MigrateAspectsStep` reads every `embed` and `globalTags` aspect and upgrades through the mutator chain. Concurrent writes from the IO-pool harness race with the sweep; `ConditionalWriteValidator` rejects stale sweep writes (tracked in `ctx.io_write_results`).
- **Catch-up backfill** — `IncrementalReindexCatchUpStep` backfills the `[T0, T1]` gap window into the new index. Each per-index window is parsed from the log line `Catch-up for entity index {name}: window [{T0}, {T1}]` and stored in `ctx.upgrade_nonblocking.catch_up_windows`.
- **Dual-write disable** — for indices where rollback dual-write is no longer needed, the upgrade marks them `DUAL_WRITE_DISABLED`. The framework parses `Marked index {name} as DUAL_WRITE_DISABLED` from upgrade-job logs and stores the names in `ctx.upgrade_nonblocking.dual_write_disabled_indices`.
- **Post-sweep upgrade-result capture** — on sweep `COMPLETED`, the phase queries MySQL for the resulting `DataHubUpgradeResult` aspect, parses `indicesState`, and stores the structured view in `ctx.upgrade_nonblocking.indices` (symmetric to `ctx.upgrade_blocking.indices` from Phase 4).

The phase preserves all legacy IO-harness captures (`ctx.io_observations`, `ctx.io_write_results`, `ctx.sweep_total_migrated`) for Suite A scenario validation. The deterministic race window (`pre_write_delay_ms = 500`) is unchanged.

If the upgrade subprocess fails or times out, the phase reports `failed` and the IO harness is shut down cleanly.

### Phase 9: Runtime Migration

`RuntimeMigrationPhase` runs lightweight read/write probes against the live NEW GMS after Phase 8 completes. It captures the schema versions observed via the read-path and write-path mutator chains into `ctx.runtime_migration`. The phase always reports `passed` — failures are recorded as probe-level errors so Phase 10 (Validation) can produce a unified verdict alongside other validator findings.

The phase verifies (via captured probes consumed by Phase 10):

- **Read-path mutator** — re-reads the first N seeded entities (default 5) through GMS and records their observed `schemaVersion`. The read-path mutator should produce the entity's `expected_schema_version` regardless of the underlying DB row's stored shape.
- **Write-path mutator** — issues N fresh writes (default 3) against URNs in the disjoint `urn:li:dashboard:(test,zdu-rt-{i})` namespace, then reads them back. The persisted `schemaVersion` should equal the post-mutator-chain target (default v4).

Probe results are surfaced in the JSON report under `phases[*].details` with counters: `read_probes`, `read_passed`, `write_probes`, `write_passed`. The probe-level detail (per-URN observed/expected versions) lives on `ctx.runtime_migration` and is consumed by Phase 10.

### Phase 10: Validation Dimension 4

`ValidationPhase._check_es_field_presence` queries the per-entity ES alias for every scenario with `expected_es_fields` set on `ZDUTestScenario`, and verifies each named field is present in the document's `_source`.

**Opting a scenario in** — add `expected_es_fields=["..."]` to the scenario in `framework/scenarios.py`:

```python
_aspect_migration(
    tc=15,
    name="ES reindexing after migration",
    aspect_name="embed",
    entity_type="dashboard",
    action="es",
    expected_schema_version=4,
    expected_es_fields=["urn"],   # always present in dashboardindex_v2 _source
),
```

Behaviour:

- Scenarios with `expected_es_fields=None` (the default) are skipped — no ES query, no FAIL records.
- Scenarios with `expected_es_fields=[]` (empty list) are queried but assert no specific fields. The alias / doc-id derivation is still verified.
- The entity-type → alias mapping lives at `ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS`. Only `dashboard` and `dataset` are in the map today; add new mappings as new entity types enter Suite A.
- ES query failures (network errors, exceptions) are recorded as FAIL records, not raised — Phase 10 does not fail-fast.
- Document-not-found at the alias is also FAIL — the sweep + MAE indexing is expected to have populated the doc by Phase 10.

### Phase 10: Validation Dimension 5

`ValidationPhase._check_dual_write_state` walks the captures laid down by Phases 4 (`upgrade_blocking`), 6 (`rolling_restart`), and 8 (`upgrade_nonblocking`) and emits one `ValidationResult` per invariant violation. Invariants:

1. Every alias in `ctx.upgrade_blocking.indices` has a non-empty `next_index_name` (alias swap target present).
2. Every name in `ctx.upgrade_nonblocking.dual_write_disabled_indices` corresponds to a tracked `old_backing_index_name` from Phase 4 (sanity).
3. If rolling restart ran, every old index has a `dualWriteStartTime` recorded in `ctx.rolling_restart.dual_write_start_times`.
4. The alias set captured by Phase 4 (blocking) equals the set captured by Phase 8 (non-blocking).
5. (Informational SKIP, not FAIL) An empty `dual_write_disabled_indices` on a non-empty stack — expected on single-image dev runs where no rolling restart occurred, so no `dualWriteStartTime` was recorded and no indices are eligible for the DUAL_WRITE_DISABLED transition. The JSON report flags this explicitly so consumers can distinguish "not captured" from "captured as empty".

---

## Configuration

### Environment Variables

All environment variables are read by `ZDUTestConfig.from_env()` at startup.

| Variable                     | Default                       | Description                                                                                                                                                                                                                                                                                                       |
| ---------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_GMS_URL`            | `http://localhost:8080`       | GMS base URL                                                                                                                                                                                                                                                                                                      |
| `DATAHUB_GMS_TOKEN`          | _(none)_                      | Bearer token for authenticated instances                                                                                                                                                                                                                                                                          |
| `ZDU_SWEEP_TIMEOUT`          | `600`                         | Seconds to wait for the sweep to complete before timing out                                                                                                                                                                                                                                                       |
| `ZDU_SKIP_PHASES`            | _(none)_                      | Comma-separated phase names to skip (see [Phases](#phases))                                                                                                                                                                                                                                                       |
| `ZDU_READER_WORKERS`         | `3`                           | Number of concurrent reader threads during sweep                                                                                                                                                                                                                                                                  |
| `ZDU_WRITER_WORKERS`         | `2`                           | Number of concurrent writer threads during sweep                                                                                                                                                                                                                                                                  |
| `ZDU_PRE_WRITE_DELAY_MS`     | `500`                         | Milliseconds injected into the upgrade container as `SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS` — see [How Concurrent I/O Works](#how-concurrent-io-works-during-sweep)                                                                                                                                    |
| `ZDU_PROJECT_DIR`            | `docker/profiles/`            | Docker Compose project directory (absolute path)                                                                                                                                                                                                                                                                  |
| `ZDU_GMS_SERVICE`            | `datahub-gms-debug`           | Compose service name for GMS (used for log tailing and secret extraction)                                                                                                                                                                                                                                         |
| `ZDU_UPGRADE_SERVICE`        | `system-update-debug`         | Compose service name for the upgrade job                                                                                                                                                                                                                                                                          |
| `ZDU_REBUILD_CWD`            | _(repo root)_                 | Working directory for `scripts/dev/datahub-dev.sh rebuild --wait`                                                                                                                                                                                                                                                 |
| `ZDU_SKIP_BOOTJAR`           | _(unset)_                     | Set to `1` to skip the automatic `./gradlew :datahub-upgrade:bootJar` that runs on `ZDUTestRunner` construction. Useful in CI where the JAR is pre-built or when iterating on Python-only changes.                                                                                                                |
| `ZDU_SKIP_BUILD_IMAGES`      | _(unset)_                     | Set to `1` to skip Phase 0 (`BuildImagesPhase`). Default-on; opt out when iterating on Python-only changes and the existing image tags are sufficient.                                                                                                                                                            |
| `ZDU_BUILD_IMAGES`           | _(unset)_                     | Backward-compat: was the Plan-13 opt-in. Now a no-op (Phase 0 is default-on). Accepted for compatibility.                                                                                                                                                                                                         |
| `ZDU_OLD_REF`                | `master`                      | Git ref to check out into the OLD worktree for Phase 0 (`BuildImagesPhase`). Override with a release tag like `v1.5.0` to build OLD from a specific release.                                                                                                                                                      |
| `ZDU_BUILD_IMAGES_ROOT`      | `smoke-test/build/zdu-images` | Root directory for the persistent OLD/NEW worktrees. Phase 0 creates `{root}/old/` and `{root}/new/` and re-uses them across runs (synced via `git fetch + reset --hard`).                                                                                                                                        |
| `ZDU_SKIP_PREPARE_OLD_STACK` | _(unset)_                     | Set to `1` to skip Phase 0.5 (`PrepareOldStackPhase`). Default-on; opt out when the running stack is already known to be on the OLD image tag.                                                                                                                                                                    |
| `DATAHUB_LOCAL_COMMON_ENV`   | `empty.env`                   | Compose env-file override loaded into every DataHub service. Set to `zdu-test.env` (tracked at `docker/profiles/zdu-test.env`) to bypass master HEAD's `PrivilegeConstraintsValidator` and default REST authorizer — required when running ZDU against current master, otherwise seed/inject phases get HTTP 403. |

### CLI Arguments (`__main__.py`)

| Argument           | Default                 | Description                                  |
| ------------------ | ----------------------- | -------------------------------------------- |
| `--gms-url`        | `http://localhost:8080` | GMS base URL                                 |
| `--gms-token`      | _(none)_                | Bearer token                                 |
| `--skip PHASE …`   | _(none)_                | One or more phase names to skip              |
| `--sweep-timeout`  | `600`                   | Seconds before sweep is considered timed-out |
| `--reader-workers` | `3`                     | Concurrent readers during sweep              |
| `--writer-workers` | `2`                     | Concurrent writers during sweep              |
| `--only-tc TC …`   | _(all)_                 | Run only the listed TC numbers               |

---

## Phases

The test pipeline runs five phases in order. Any phase can be skipped individually.

| Phase name   | What it does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `discovery`  | Snapshots the Docker image digest of every running Compose service. Verifies that the GMS container is up before proceeding.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `seed`       | Ingests one entity per test scenario using the scenario's starting `schemaVersion`. Also seeds 5 dedicated IO-pool entities (`zdu-io-pool-{0..4}`) **directly into MySQL** (bypassing GMS write-path mutators) so they always land at `schemaVersion=null` (v1) in the DB regardless of `ASPECT_MIGRATION_MUTATOR_ENABLED`.                                                                                                                                                                                                                             |
| `upgrade`    | Runs `scripts/dev/datahub-dev.sh rebuild --wait` to build and restart GMS with the new code. Waits for GMS health check. Warns if `ASPECT_MIGRATION_MUTATOR_ENABLED` is not active on the new GMS.                                                                                                                                                                                                                                                                                                                                                      |
| `tell`       | Launches `docker compose run --rm system-update-debug` with `SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED=true` and `SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS=<ZDU_PRE_WRITE_DELAY_MS>`. Reader threads continuously read all seeded entities. Writer threads are sweep-synchronized: they block on `Processing batch URNs` log events, then fire writes to those exact URNs inside the pre-write delay window — guaranteeing the concurrent write beats the sweep write on every batch. Phase completes when the sweep emits a `COMPLETED` log event. |
| `validation` | Reads back each seeded entity and checks that `schemaVersion` equals the expected value for that scenario.                                                                                                                                                                                                                                                                                                                                                                                                                                              |

### Skipping Phases

Specify a comma-separated list in `ZDU_SKIP_PHASES`, or pass `--skip` on the CLI:

```bash
# Skip the sweep — validate only (assumes a prior run already upgraded)
ZDU_SKIP_PHASES=upgrade_nonblocking \
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/test_zdu_upgrade.py -v

# CLI equivalent
python -m tests.zdu --skip upgrade_nonblocking
```

Available phase names: `build_images`, `prepare_old_stack`, `discovery`, `seed`, `snapshot_t0`, `upgrade_blocking`, `inject_traffic_pre`, `rolling_restart`, `inject_traffic_dual`, `upgrade_nonblocking`, `runtime_migration`, `validation`

---

## Running Specific Test Cases

Filter by TC number using `ZDU_SKIP_TC` or `--only-tc`:

```bash
# Run only TC-001 through TC-006 (single-hop and multi-hop basic cases)
python -m tests.zdu --only-tc 1 2 3 4 5 6

# Via pytest (parametrized, use -k to filter by TC ID)
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py \
  -k "TC-001 or TC-004 or TC-007" -v
```

---

## Test Scenarios

### TC-001 to TC-003 — Single Hop Migration (`globalTags`, `dataset`)

| TC  | Name                  | Action | Start Version | Expected Version |
| --- | --------------------- | ------ | ------------- | ---------------- |
| 001 | Full sweep single hop | sweep  | null (v1)     | 2                |
| 002 | Read path single hop  | read   | null (v1)     | 2                |
| 003 | Write path single hop | write  | null (v1)     | 2                |

### TC-004 to TC-023 — Multi Hop Migration (`embed`, `dashboard`)

| TC  | Name                          | Action    | Start Version | Expected Version | Notes                                         |
| --- | ----------------------------- | --------- | ------------- | ---------------- | --------------------------------------------- |
| 004 | Full sweep multi hop          | sweep     | null (v1)     | 4                |                                               |
| 005 | Read path multi hop           | read      | null (v1)     | 4                |                                               |
| 006 | Write path multi hop          | write     | null (v1)     | 4                |                                               |
| 007 | Mid-chain start at v2         | sweep     | 2             | 4                | Requires `bridgeGap` (see Known Failures)     |
| 008 | Already at target v4          | sweep     | 4             | 4                | No-op                                         |
| 009 | Future version v5             | sweep     | 5             | 5                | All mutators skipped                          |
| 010 | Null systemMetadata           | sweep     | null          | 4                | Treated as v1                                 |
| 011 | Gap in mutator chain          | sweep     | 2             | 4                | Requires `bridgeGap` (see Known Failures)     |
| 012 | transform() returns null      | sweep     | 3             | 3                | XFAIL — null is no-op by design               |
| 013 | Invalid URN crashes sweep     | sweep     | null          | —                | XFAIL — not expected in prod                  |
| 014 | Malformed JSON in metadata    | sweep     | null          | —                | XFAIL — not expected in prod                  |
| 015 | ES reindexing after migration | es        | null (v1)     | 4                | Checks Elasticsearch doc counts               |
| 016 | Re-run after SUCCEEDED        | lifecycle | —             | —                | Sweep emits SKIPPED event                     |
| 017 | ABORTED treated as terminal   | lifecycle | —             | —                | ABORTED = SUCCEEDED for skip logic            |
| 018 | IN_PROGRESS resumes sweep     | lifecycle | —             | —                | Resumes from saved cursor                     |
| 019 | chain.disable() not wired     | sweep     | —             | —                | XFAIL — known open bug                        |
| 020 | Read path in-memory only      | read      | 1             | 4                | DB stays at v1; API read returns v4 in-memory |
| 021 | Write path persists           | write     | 1             | 4                | Requires `bridgeGap` (see Known Failures)     |
| 022 | APP_SOURCE stamped on sweep   | integrity | —             | —                | systemMetadata.properties check               |
| 023 | Rolling upgrade               | rolling   | —             | —                | SKIP — test steps not yet defined             |

### TC-301 to TC-309 — Suite D — ES Phase 2 Catch-Up

| TC  | Name                                           | Dev stack | Notes                                                                          |
| --- | ---------------------------------------------- | --------- | ------------------------------------------------------------------------------ |
| 301 | Entity index gap catch-up                      | XFAIL     | Requires two-image rolling restart (`ZDU_OLD_IMAGE_TAG` / `ZDU_NEW_IMAGE_TAG`) |
| 302 | Timeseries catch-up via filtered `_reindex`    | XFAIL     | Requires two-image + timeseries seed harness                                   |
| 303 | Global graph index catch-up                    | XFAIL     | Requires two-image rolling restart                                             |
| 304 | Global system metadata index catch-up          | XFAIL     | Requires two-image rolling restart                                             |
| 305 | T0 >= T1 no-op                                 | PASS      | Dev stack records empty `catch_up_windows` — invariant holds                   |
| 306 | No Phase 1 result no-op                        | PASS      | Empty `blocking.indices` ⇒ empty `catch_up_windows`                            |
| 307 | Resume from `lastUrn` checkpoint               | XFAIL     | Requires upgrade-job kill-switch                                               |
| 308 | DUAL_WRITE_DISABLED set when rollback flag off | XFAIL     | Requires runtime config knob for `rollbackDualWriteEnabled`                    |
| 309 | DUAL_WRITE_DISABLED NOT set when flag on       | XFAIL     | Requires runtime config knob for `rollbackDualWriteEnabled`                    |

**Validators read from captures already on `ctx`:**

- `ctx.gap_urns` — populated by Plan 5 `InjectTrafficPrePhase` (10 URNs written via OLD GMS in the T0–T1 window).
- `ctx.upgrade_nonblocking.catch_up_windows` — Plan 7 `UpgradeNonBlockingPhase` parses `Catch-up for entity index {name}: window [{T0}, {T1}]` from the upgrade-job log.
- `ctx.upgrade_nonblocking.dual_write_disabled_indices` — Plan 7 parses `Marked index {name} as DUAL_WRITE_DISABLED`.
- `ctx.upgrade_nonblocking.indices` — Plan 7 reads the post-sweep `DataHubUpgradeResult.indicesState` from MySQL.

When run against a real two-image-tag CI stack, the XFAIL scenarios flip to PASS without any code change — the active validators (TC-305, TC-306) and the XFAIL stubs both gate on capture-driven invariants that hold whenever the captures are non-empty.

### TC-101 to TC-112 — Suite B — ES Phase 1 Incremental Reindex

| TC  | Name                                     | Dev stack | Notes                                                       |
| --- | ---------------------------------------- | --------- | ----------------------------------------------------------- |
| 101 | Single-index reindex with mapping change | XFAIL     | Requires distinct OLD/NEW image mappings to trigger reindex |
| 102 | No-reindex needed                        | XFAIL     | Requires identical-mappings setup                           |
| 103 | Settings/mappings-only update            | XFAIL     | Requires distinct mappings                                  |
| 104 | Empty source index                       | XFAIL     | Requires explicit doc-drop step before upgrade              |
| 105 | Multiple indices, all need reindex       | XFAIL     | Requires multi-index mapping setup                          |
| 106 | Mixed reindex / mapping-only             | XFAIL     | Requires multi-index mapping setup                          |
| 107 | Timeseries index reindex                 | XFAIL     | Requires timeseries entity seed harness                     |
| 108 | `DataHubUpgradeResult` state shape       | PASS      | Active — verifies 7 required keys per `indicesState` entry  |
| 109 | Re-run after COMPLETED                   | XFAIL     | Requires a second blocking-phase run                        |
| 110 | Resume after interruption                | XFAIL     | Requires fault injection / kill-switch                      |
| 111 | Reindex failure → cleanup                | XFAIL     | Requires ES fault injection                                 |
| 112 | Doc count mismatch fails alias swap      | XFAIL     | Requires mid-flight ES doc deletion                         |

**Active validator (TC-108)** reads from `ctx.upgrade_blocking.raw["indicesState"]` (Plan 2) and asserts every entry has the 7 keys: `nextIndexName`, `oldBackingIndexName`, `reindexStartTime`, `sourceDocCount`, `taskId`, `requiresDataBackfill`, `status`.

### TC-401 to TC-408 — Suite E — System-Level Sweep

| TC  | Name                                      | Dev stack | Notes                                                                    |
| --- | ----------------------------------------- | --------- | ------------------------------------------------------------------------ |
| 401 | Sweep cursor resumability                 | XFAIL     | Requires upgrade-job kill-switch                                         |
| 402 | Sweep respects batchDelayMs               | XFAIL     | Requires wall-clock instrumentation                                      |
| 403 | Sweep skips already-migrated rows         | XFAIL     | Requires pre-migration setup + metric capture                            |
| 404 | Sweep with no mutators registered         | PASS/SKIP | Active — PASS if `chain empty`; SKIP if chain has mutators (typical dev) |
| 405 | Sweep with feature flag off               | XFAIL     | Requires runtime config knob                                             |
| 406 | APP_SOURCE stamped on sweep writes        | XFAIL     | Already covered by Suite A TC-022 (different angle)                      |
| 407 | IF_VERSION_MATCH header prevents stomping | XFAIL     | Requires race-window proof                                               |
| 408 | Chain disable after sweep completes       | XFAIL     | Requires `AspectMigrationMutatorChain disabled` log capture              |

**Active validator (TC-404)** reads from `ctx.upgrade_nonblocking.{indices, dual_write_disabled_indices}` and `ctx.sweep_total_migrated` (all from Plan 7). When all three are zero, the no-mutator no-op is confirmed (PASS); otherwise the precondition isn't met (SKIP — not FAIL, since the dev stack typically does have mutators registered).

### TC-501 to TC-507 — Suite F — Live Traffic Under Migration

| TC  | Name                                      | Dev stack | Notes                                                               |
| --- | ----------------------------------------- | --------- | ------------------------------------------------------------------- |
| 501 | Reads return new format mid-sweep         | XFAIL     | Requires sustained 50 RPS read load generator + p99 latency capture |
| 502 | Writes persist as new format mid-sweep    | XFAIL     | Requires sustained 10 RPS write load generator                      |
| 503 | Read consistency across sweep boundary    | XFAIL     | Requires sequential-read instrumentation                            |
| 504 | Sweep + concurrent writes don't lose data | PASS      | Active — every `ctx.io_write_results` entry must have `passed=True` |
| 505 | ES dual-write under live load             | XFAIL     | Requires ES doc-count parity check across both physical indices     |
| 506 | Read sees catch-up in progress            | XFAIL     | Requires catch-up timing instrumentation                            |
| 507 | Ingestion run during ZDU                  | XFAIL     | Requires ingestion job harness (csv-enricher)                       |

**Active validator (TC-504)** reads `ctx.io_write_results` (Plan 7 IO harness). When all entries have `passed=True`, the sweep preserved every concurrent client write (PASS). On any `passed=False`, FAIL with the first failed URN + observed/expected version + error in `actual_result`. SKIP when no harness writes were captured (Phase 8 was skipped).

---

## Understanding Results

### Scenario Statuses

| Status  | Meaning                                                                        |
| ------- | ------------------------------------------------------------------------------ |
| `PASS`  | Entity reached the expected `schemaVersion`                                    |
| `FAIL`  | Entity is at the wrong version; see `failure_reason`                           |
| `XFAIL` | Known failure — scenario is listed in `KNOWN_FAILURES` in `scenario_loader.py` |
| `XPASS` | Scenario expected to fail but passed — consider removing from `KNOWN_FAILURES` |
| `SKIP`  | Scenario has no seeded entity (e.g., TC-023 rolling upgrade)                   |

### Phase Statuses

| Status   | Meaning                                                                                    |
| -------- | ------------------------------------------------------------------------------------------ |
| `passed` | Phase completed successfully                                                               |
| `failed` | Phase hit an error; `error` field has details. Stops the run if `fail_fast=True` (default) |

### Report File

A JSON report is written to `smoke-test/build/zdu-test-report.json` after every run:

```json
{
  "config": { "gms_url": "...", "sweep_timeout_s": 600, ... },
  "phase_results": [
    { "phase_name": "discovery", "status": "passed", "duration_s": 0.7, "details": {...} },
    ...
  ],
  "scenario_results": [
    { "tc_number": 1, "name": "Full sweep single hop", "status": "PASS", ... },
    ...
  ],
  "started_at": "...",
  "ended_at": "..."
}
```

### Failure Artifact Bundle

When any phase or scenario fails, the framework writes a structured artifact bundle to `smoke-test/build/zdu-failure-{timestamp}/` containing everything an operator needs to triage without re-running the test:

```
zdu-failure-20260510T142503Z/
├── summary.json                      list of failed phases + scenarios with reasons
├── compose-logs/
│   ├── datahub-gms-debug.log         tail 2000 lines from each running service
│   ├── datahub-mae-consumer-debug.log
│   ├── datahub-mce-consumer-debug.log
│   ├── mysql.log
│   └── opensearch.log
├── es-cat-indices.txt                GET /_cat/indices?v
├── es-cat-aliases.txt                GET /_cat/aliases?v
├── upgrade-result.json               { "blocking": ..., "nonblocking": ... } from MySQL DataHubUpgradeResult
├── mysql-aspects.csv                 every metadata_aspect_v2 row matching zdu-tc-* / zdu-io-pool-* / zdu-gap-* / zdu-dual-* / zdu-rt-*
└── reader-writer-events.jsonl        ctx.io_observations + ctx.io_write_results, one event per line
```

The bundle is best-effort — if a per-artifact dump fails (e.g., ES unreachable, container gone), the failure is logged and the bundle is still written with the remaining artifacts. Missing artifacts are preferable to no bundle at all.

CI uploads `smoke-test/build/zdu-failure-*/` as a build artifact on test failure, so operators can download and inspect locally.

---

## How Concurrent I/O Works During Sweep

The `upgrade_nonblocking` phase runs three things simultaneously:

1. **Sweep job** — `docker compose run --rm system-update-debug` runs `MigrateAspectsStep`, which reads every `embed` and `globalTags` aspect from the DB and upgrades it through the mutator chain.

2. **Writer threads** — sweep-synchronized workers that target exactly the URNs the sweep is about to write. See [Deterministic Race Window](#deterministic-race-window) below.

3. **Reader threads** — 3 workers continuously read all seeded entities. A read of an entity at v1 must return `schemaVersion=4` (in-memory migration on the read path), even if the sweep has not yet processed that entity.

The IO-pool entities (`zdu-io-pool-{0..4}`) are intentionally separate from the scenario test entities, so concurrent writers cannot corrupt edge-case entities like TC-009 (future version v5 — must stay at v5).

**Why MySQL direct insertion?** If IO-pool entities were seeded through the GMS API while `ASPECT_MIGRATION_MUTATOR_ENABLED=true`, the write-path mutator chain would immediately upgrade them to `schemaVersion=4` before they reach the DB. The sweep would never see them in its batch (it only processes aspects below the target version). By inserting directly into `metadata_aspect_v2` with `systemmetadata='{}'` (no `schemaVersion`), the entities remain at v1 in the DB and are guaranteed to appear in the sweep's batch.

**Plan F-5 closure:** The implementation now matches this design. `SeedPhase._seed_io_pool_via_mysql` calls `MySQLClient.upsert_aspect_raw` to write each IO-pool URN directly to `metadata_aspect_v2`, bypassing GMS entirely. This makes the race-window assertion (TC-403) reproducible across single-image dev runs and two-image-tag CI runs, regardless of the seed-time GMS's `ASPECT_MIGRATION_MUTATOR_ENABLED` flag. Closes Notion review thread D5 ("Test mutator for race window").

### Deterministic Race Window

The framework guarantees that a concurrent client write lands **inside** the sweep's read→write gap on every batch — not just probabilistically.

**How it works end-to-end:**

```
Test config: pre_write_delay_ms = 500
      ↓
UpgradeNonBlockingPhase passes to docker compose run:
  -e SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS=500
      ↓
MigrateAspectsStep (in upgrade container) reads it via System.getenv()
```

Inside each sweep batch:

```
MigrateAspectsStep (upgrade container):
  log.info("Processing batch URNs: urn:li:dashboard:(test,zdu-io-pool-0),...")  ← always logged
  Thread.sleep(500ms)   ← race window is open (only when PRE_WRITE_DELAY_MS > 0)
  entityService.ingestProposal(...)

LogMonitor._tail_popen (Python, reading upgrade container stdout):
  parses "Processing batch URNs" line → SweepState.BATCH_URNS event
  emits one log line per URN:
    [upgrade] sweep URN: urn:li:dashboard:(test,zdu-io-pool-0)
    [upgrade] sweep URN: urn:li:dashboard:(test,zdu-io-pool-1)
    ...

Test writer thread (consuming BATCH_URNS events from LogMonitor):
  ← sees BATCH_URNS event with ["urn:li:dashboard:(test,zdu-io-pool-0)", ...]
  ← fires ingest_mcp() to each IO-pool URN immediately (~10-50ms)
  ← GMS logs: [gms] Producing MCL for ingested aspect embed, urn urn:li:dashboard:(test,zdu-io-pool-0)
  ← write completes, entity version bumped to N+1
  ← sleep ends, sweep calls ingestProposal with If-Version-Match: N
  ← ConditionalWriteValidator: stored version N+1 ≠ header N → REJECTS sweep write
```

The `[upgrade] sweep URN:` and `[gms] Producing MCL ...` lines share the same URN, making the race visible line-by-line in the test output. Only IO-pool URNs trigger concurrent writes; TC scenario entity URNs visible in `[upgrade]` have no corresponding `[gms]` write because writers exclusively target the IO-pool.

The client write always wins. `ConditionalWriteValidator` silently skips the stale sweep write (non-API context — no exception thrown, no crash).

**`SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS` is set automatically** by the test framework — you never set it manually. Control it via `ZDU_PRE_WRITE_DELAY_MS` (default `500`). Setting it to `0` disables the delay and falls back to probabilistic overlap.

---

## Scenario Source

Scenarios are codified as Python objects in `framework/scenarios.py`. Each scenario is a `ZDUTestScenario` instance with explicit fields (TC#, name, aspect, expected schemaVersion, etc.).

To add a new scenario, append a new constructor call to `SUITE_A_SCENARIOS`. To mark a TC as XFAIL, add it to `KNOWN_FAILURES` in `framework/scenario_loader.py`.

---

## Troubleshooting

**Sweep times out (600s)**

The upgrade job container requires `DATAHUB_TOKEN_SERVICE_SIGNING_KEY` and `DATAHUB_TOKEN_SERVICE_SALT`. The framework reads these from the running GMS container automatically via `docker inspect`. If the GMS container is not running, the upgrade job will fail to start. Ensure DataHub is up before running the tests.

**Sweep times out with `MigrateAspects: no mutators registered — nothing to migrate`**

The `system-update-debug` compose service mounts `datahub-upgrade/build/libs/` directly into the container. If that directory contains a stale `datahub-upgrade.jar` compiled before the current `SpringStandardPluginConfiguration` snapshot, the chain comes up empty (`mutator(s)=0`) and `MigrateAspectsStep` no-ops, but the framework keeps waiting for `Sweep complete. Total migrated: N` until `sweep_timeout_s` (600s) elapses.

`ZDUTestRunner.__init__` now runs `./gradlew :datahub-upgrade:bootJar` automatically before each test session to prevent this. Gradle is incremental — when nothing changed it's a ~14s no-op. Set `ZDU_SKIP_BOOTJAR=1` to opt out (e.g., in CI where the JAR is pre-built, or when iterating on Python-only changes).

Manual fix (if the auto-rebuild is opted out and you hit the symptom):

```bash
./gradlew :datahub-upgrade:bootJar
```

The compose volume mount picks up the fresh JAR on the next `docker compose run` — no image rebuild or stack restart needed. Verify by running the framework and confirming `Initialized AspectMigrationMutatorChain: aspectMigrationMutatorEnabled=true, mutator(s)=3` in the upgrade-job log (look for the line via `docker logs <upgrade-container-name> | grep AspectMigrationMutatorChain`).

**`ASPECT_MIGRATION_MUTATOR_ENABLED=false` warning after upgrade**

The rebuild did not include the flag. Set it in the compose override before rebuilding:

```bash
scripts/dev/datahub-dev.sh env set ASPECT_MIGRATION_MUTATOR_ENABLED=true
scripts/dev/datahub-dev.sh env restart
```

**Sweep emits SKIPPED (prior SUCCEEDED result)**

The framework automatically detects this, deletes the prior `urn:li:dataHubUpgrade:migrate-aspects-<version>` URN, and retries with a new timestamp-based version. If it still skips, check the DB for stale upgrade URNs.

**401 Unauthorized on seed**

Run `datahub init --username datahub --password datahub` and set `DATAHUB_GMS_TOKEN` to the returned token.

**Rebuild fails with `rc=127` (no such file or directory)**

The rebuild command runs from the repo root by default (`ZDU_REBUILD_CWD`). Ensure you are not overriding it to a subdirectory that does not contain `scripts/dev/datahub-dev.sh`.
