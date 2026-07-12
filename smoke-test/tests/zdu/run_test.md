# ZDU E2E Test Framework — Configuration & Running Guide

---

## Prerequisites

| Requirement                | Notes                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------- |
| Docker + Docker Compose v5 | `docker compose` must be on `$PATH`                                                    |
| DataHub running locally    | `scripts/dev/datahub-dev.sh start`                                                     |
| Python venv                | `smoke-test/venv/bin/python` — created by Gradle or `scripts/dev/datahub-dev.sh setup` |
| GMS token                  | `datahub init --username datahub --password datahub` once                              |

---

## Configuration

### Environment Variables

All consumed by `ZDUTestConfig.from_env()` at startup.

| Variable                           | Default                       | Description                                                                                                                                                                                                                                                                                                                                     |
| ---------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_GMS_URL`                  | `http://localhost:8080`       | GMS base URL                                                                                                                                                                                                                                                                                                                                    |
| `DATAHUB_GMS_TOKEN`                | _(none)_                      | Bearer token for authenticated instances                                                                                                                                                                                                                                                                                                        |
| `ZDU_SWEEP_TIMEOUT`                | `600`                         | Seconds to wait for sweep before timing out                                                                                                                                                                                                                                                                                                     |
| `ZDU_SKIP_PHASES`                  | _(none)_                      | Comma-separated phase names to skip                                                                                                                                                                                                                                                                                                             |
| `ZDU_READER_WORKERS`               | `3`                           | Concurrent reader threads during sweep                                                                                                                                                                                                                                                                                                          |
| `ZDU_WRITER_WORKERS`               | `2`                           | Concurrent writer threads during sweep                                                                                                                                                                                                                                                                                                          |
| `ZDU_PRE_WRITE_DELAY_MS`           | `500`                         | Race-window delay injected into upgrade container                                                                                                                                                                                                                                                                                               |
| `ZDU_PROJECT_DIR`                  | `docker/profiles/`            | Docker Compose project directory (absolute path)                                                                                                                                                                                                                                                                                                |
| `ZDU_GMS_SERVICE`                  | `datahub-gms-debug`           | Compose service name for GMS                                                                                                                                                                                                                                                                                                                    |
| `ZDU_UPGRADE_SERVICE`              | `system-update-debug`         | Compose service name for the upgrade job                                                                                                                                                                                                                                                                                                        |
| `ZDU_OLD_IMAGE_TAG`                | `debug`                       | Tag for OLD-image side (two-image-tag testing)                                                                                                                                                                                                                                                                                                  |
| `ZDU_NEW_IMAGE_TAG`                | `debug`                       | Tag for NEW-image side                                                                                                                                                                                                                                                                                                                          |
| `ZDU_REBUILD_CWD`                  | _(repo root)_                 | Working dir for `scripts/dev/datahub-dev.sh rebuild --wait`                                                                                                                                                                                                                                                                                     |
| `ZDU_SKIP_BOOTJAR`                 | _(unset)_                     | Set to `1` to skip auto `./gradlew :datahub-upgrade:bootJar` on runner construction                                                                                                                                                                                                                                                             |
| `ZDU_SKIP_BUILD_IMAGES`            | _(unset)_                     | Set to `1` to skip Phase 0 `BuildImagesPhase`. Default-on; opt out for Python-only iterations                                                                                                                                                                                                                                                   |
| `ZDU_BUILD_IMAGES`                 | _(unset)_                     | Backward-compat no-op (Phase 0 is now default-on). Accepted for compatibility                                                                                                                                                                                                                                                                   |
| `ZDU_OLD_REF`                      | `master`                      | Git ref the OLD worktree is checked out at (e.g., `v1.5.0`, a branch, a SHA)                                                                                                                                                                                                                                                                    |
| `ZDU_BUILD_IMAGES_ROOT`            | `smoke-test/build/zdu-images` | Root directory for persistent OLD/NEW worktrees (`{root}/old/`, `{root}/new/`)                                                                                                                                                                                                                                                                  |
| `ZDU_SKIP_PREPARE_OLD_STACK`       | _(unset)_                     | Set to `1` to skip Phase 0.5 `PrepareOldStackPhase`. Default-on; opt out when stack is already OLD                                                                                                                                                                                                                                              |
| `DATAHUB_LOCAL_COMMON_ENV`         | `empty.env`                   | Compose env-file override path (relative to `docker/profiles/`). **Set to `zdu-test.env` when running against current master HEAD** — it loads `AUTH_POLICIES_ENABLED=false` + `REST_API_AUTHORIZATION_ENABLED=false` to bypass `PrivilegeConstraintsValidator` and the default REST authorizer. Without this, seed/inject phases get HTTP 403. |
| `ASPECT_MIGRATION_MUTATOR_ENABLED` | _(GMS default)_               | Forwarded to the upgrade container — controls mutator-chain registration                                                                                                                                                                                                                                                                        |

### CLI Arguments (`__main__.py`)

| Flag                              | Description                         |
| --------------------------------- | ----------------------------------- |
| `--gms-url URL`                   | GMS base URL                        |
| `--gms-token TOK`                 | Bearer token                        |
| `--skip PHASE [PHASE …]`          | Phase names to skip (overrides env) |
| `--sweep-timeout N`               | Override `ZDU_SWEEP_TIMEOUT`        |
| `--reader-workers N`              | Override `ZDU_READER_WORKERS`       |
| `--writer-workers N`              | Override `ZDU_WRITER_WORKERS`       |
| `--only-tc N [N …]`               | Run only the listed TC numbers      |
| `--suite {a,b,c,d,e,f,g,h} [...]` | Run only the listed suites          |

---

## Pipeline Phases (run order)

| #   | Phase                 | Class                     | Skippable | Notes                                                                                                |
| --- | --------------------- | ------------------------- | --------- | ---------------------------------------------------------------------------------------------------- |
| 0   | `build_images`        | `BuildImagesPhase`        | ✓         | Default-on. Builds OLD/NEW images from persistent worktrees. Mutates `old_image_tag`/`new_image_tag` |
| 0.5 | `prepare_old_stack`   | `PrepareOldStackPhase`    | ✓         | Default-on. Recreates running Compose services onto `config.old_image_tag` if mismatched             |
| 1   | `discovery`           | `DiscoveryPhase`          | ✓         | Snapshots image digests for every Compose service                                                    |
| 2   | `seed`                | `SeedPhase`               | ✓         | Seeds scenario entities via GMS API; IO pool via direct MySQL                                        |
| 3   | `snapshot_t0`         | `SnapshotT0Phase`         | ✓         | Captures ES doc counts, aspect counts, mappings hash                                                 |
| 4   | `upgrade_blocking`    | `UpgradeBlockingPhase`    | ✓         | Runs `system-update -u SystemUpdateBlocking`, captures `indicesState`                                |
| 5   | `inject_traffic_pre`  | `InjectTrafficPrePhase`   | ✓         | Fires 10 writes via OLD GMS (T0–T1 gap window)                                                       |
| 6   | `rolling_restart`     | `RollingRestartPhase`     | ✓         | Swaps GMS → MAE → MCE OLD→NEW; records `dualWriteStartTime`                                          |
| 7   | `inject_traffic_dual` | `InjectTrafficDualPhase`  | ✓         | Fires 10 writes via NEW GMS (dual-write window)                                                      |
| 8   | `upgrade_nonblocking` | `UpgradeNonBlockingPhase` | ✓         | Runs `system-update -u SystemUpdateNonBlocking` + concurrent IO                                      |
| 9   | `runtime_migration`   | `RuntimeMigrationPhase`   | ✓         | Read/write probes through GMS (mutator chain assertion)                                              |
| 10  | `validation`          | `ValidationPhase`         | ✓         | Per-TC validation across all 5 dimensions                                                            |

Phase 0 and 0.5 are **default-on** and form the build+prepare entry to a one-button workflow. Opt out via `ZDU_SKIP_BUILD_IMAGES=1` / `ZDU_SKIP_PREPARE_OLD_STACK=1` when iterating on Python-only changes against an already-prepared stack.

The previous legacy `upgrade` phase (a `datahub-dev.sh rebuild` wrapper) has been removed — its function is now covered by Phase 0 (`build_images`) + Phase 4 (`upgrade_blocking`).

---

## Test Suites (codified)

| Suite                         | Range       | Scenarios | Active on dev                                            | Executor           |
| ----------------------------- | ----------- | --------- | -------------------------------------------------------- | ------------------ |
| **A** Aspect schema migration | TC-001..023 | 23        | 14 PASS / 7 XFAIL / 1 SKIP / 1 pre-existing FAIL         | `aspect_migration` |
| **B** ES Phase 1 reindex      | TC-101..112 | 12        | 0 PASS / 11 XFAIL / 1 SKIP (TC-108 needs `indicesState`) | `phase1_reindex`   |
| **D** ES Phase 2 catch-up     | TC-301..309 | 9         | 2 PASS (TC-305, 306) / 7 XFAIL                           | `catch_up`         |
| **E** System-level sweep      | TC-401..408 | 8         | 0 PASS / 7 XFAIL / 1 SKIP (TC-404 needs empty chain)     | `sweep`            |
| **F** Live traffic            | TC-501..507 | 7         | 1 PASS (TC-504) / 6 XFAIL                                | `live_traffic`     |

Suites **C, G, H, I** are not codified.

---

## Running

### 1. Full default run (all codified suites)

```bash
cd "$(git rev-parse --show-toplevel)"  # cd to the datahub repo root
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
smoke-test/venv/bin/python -m tests.zdu
```

Runs all 10 phases + all 59 scenarios. On a single-image dev stack, expect ~17 PASS / 38 XFAIL / 3 SKIP / 1 pre-existing FAIL.

### 2. Single suite

```bash
# Suite A only
smoke-test/venv/bin/python -m tests.zdu --suite a

# Multiple suites
smoke-test/venv/bin/python -m tests.zdu --suite a d f

# All five codified
smoke-test/venv/bin/python -m tests.zdu --suite a b d e f
```

### 3. Single test case (or subset)

```bash
# Just TC-015 (ES reindex check)
smoke-test/venv/bin/python -m tests.zdu --only-tc 15

# Multiple specific TCs
smoke-test/venv/bin/python -m tests.zdu --only-tc 1 4 15 22

# TC + suite filter (intersection)
smoke-test/venv/bin/python -m tests.zdu --suite d --only-tc 305 306
```

### 4. Skip specific phases

```bash
# Recommended dev-stack invocation — skip phases that need real two-image infra
ZDU_SKIP_PHASES=rolling_restart \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Or via CLI flag
smoke-test/venv/bin/python -m tests.zdu --skip rolling_restart

# Skip everything heavy — validation-only
ZDU_SKIP_PHASES=rolling_restart,upgrade_blocking,upgrade_nonblocking \
smoke-test/venv/bin/python -m tests.zdu --suite d
```

### 5. Run via pytest (instead of `__main__`)

```bash
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/test_zdu_upgrade.py -v
```

Pytest path uses the same `ZDUTestRunner` under the hood but gives per-phase pytest output (each phase becomes a test method).

### 6. Two-image-tag CI mode (real ZDU) — default behavior

Phase 0 (`BuildImagesPhase`) and Phase 0.5 (`PrepareOldStackPhase`) are **default-on**, so the recommended invocation needs no extra env vars:

```bash
# Builds OLD from master HEAD, NEW from current branch HEAD, prepares OLD stack,
# then runs the full ZDU pipeline.
# First run: 40-70 min cold (Gradle :docker for 4 services × 2 sides).
# Reruns: cache-hit, <2s. Phase 0.5: ~1s if stack is already OLD, ~60-120s if recreation needed.
DATAHUB_GMS_TOKEN="$TOKEN" \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Build OLD from a specific release tag instead of master
ZDU_OLD_REF=v1.5.0 \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

`BuildImagesPhase` syncs `smoke-test/build/zdu-images/old/` to the OLD ref and `smoke-test/build/zdu-images/new/` to the current branch (persistent worktrees, never deleted between runs), builds OLD with `-Ptag=zdu-old-{sha8}`, NEW with `-Ptag=zdu-new-{sha8}`, and mutates `config.old_image_tag` / `config.new_image_tag` before downstream phases construct. `PrepareOldStackPhase` then inspects the running stack and recreates any service that is not on the new OLD tag.

**Manual image management (opt out of both phases):**

```bash
# Bring up the stack with the OLD image manually
DATAHUB_VERSION=v1.5.0 scripts/dev/datahub-dev.sh start

# Run with explicit OLD/NEW tags — rolling_restart will actually swap images
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
ZDU_OLD_IMAGE_TAG=v1.5.0 \
ZDU_NEW_IMAGE_TAG=v1.6.0 \
DATAHUB_GMS_TOKEN="$TOKEN" \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

In either path, the XFAIL TCs that require a real rolling restart (TC-101..107, TC-301..304, etc.) automatically flip to active assertions.

### 7. Framework unit tests (no live stack required)

```bash
# All 258 framework unit tests
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/

# Specific test file
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_catchup_executor.py -v

# Single test
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/framework/test_phase1_reindex_executor.py::TestTC108StateShape -v
```

### 8. Opt-out of auto-bootJar (skip the ~14s gradle check)

```bash
ZDU_SKIP_BOOTJAR=1 smoke-test/venv/bin/python -m tests.zdu --suite a
```

Useful in CI where the JAR is pre-built, or when iterating on Python-only changes.

### 9. Increase verbosity / debug log

```bash
# Through Python logging
PYTHONUNBUFFERED=1 \
LOGGING_LEVEL_ROOT=DEBUG \
smoke-test/venv/bin/python -m tests.zdu --suite a 2>&1 | tee zdu-debug.log
```

---

## Output Files

| Path                                        | What                                                               |
| ------------------------------------------- | ------------------------------------------------------------------ |
| `smoke-test/build/zdu-test-report.json`     | Per-run JSON report (phases, scenarios, summary)                   |
| `smoke-test/build/zdu-failure-{TIMESTAMP}/` | Failure artifact bundle on any phase or scenario FAIL              |
| └─ `summary.json`                           | Failed phases + scenarios with reasons                             |
| └─ `compose-logs/*.log`                     | `docker compose logs --tail=2000` per service                      |
| └─ `es-cat-indices.txt`                     | `GET /_cat/indices?v`                                              |
| └─ `es-cat-aliases.txt`                     | `GET /_cat/aliases?v`                                              |
| └─ `upgrade-result.json`                    | `DataHubUpgradeResult` for blocking + nonblocking                  |
| └─ `mysql-aspects.csv`                      | Every `metadata_aspect_v2` row matching `zdu-*` prefixes           |
| └─ `reader-writer-events.jsonl`             | `ctx.io_observations` + `ctx.io_write_results`, one event per line |

Inspect the latest bundle:

```bash
BUNDLE=$(ls -1d smoke-test/smoke-test/build/zdu-failure-* | sort | tail -1)
cat "$BUNDLE/summary.json"
```

---

## Common Recipes

### Real two-image-tag ZDU run (master HEAD compatible)

Default invocation against current master — exercises Phase 0 build, Phase 0.5 stack prep, Phase 6 OLD→NEW rolling restart, and the sweep:

```bash
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}' | head -1)
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env \
smoke-test/venv/bin/python -m tests.zdu --suite a b d e f
```

First run: ~5-15 min for cold builds. Subsequent runs cache-hit and complete in ~30s.

`DATAHUB_LOCAL_COMMON_ENV=zdu-test.env` is required against current master because master HEAD's `PrivilegeConstraintsValidator` rejects all REST writes from the personal access token. The override file disables policy + REST authorization for the duration of the test.

### Quick dev sanity check (under 30 seconds)

```bash
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env \
ZDU_SKIP_PHASES=rolling_restart,upgrade_nonblocking \
smoke-test/venv/bin/python -m tests.zdu --suite a --only-tc 1
```

### Full pipeline against running stack (no rebuild)

```bash
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
DATAHUB_LOCAL_COMMON_ENV=zdu-test.env \
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
ZDU_SKIP_PHASES=rolling_restart \
smoke-test/venv/bin/python -m tests.zdu --suite a b d e f
```

Recommended single-image dev-stack invocation: Phase 0 and Phase 0.5 are opted out (existing `debug` images), and the upgrade/rolling-restart phases that need a real two-image stack are skipped. Suite A baseline preserved; all 59 scenarios run.

### Only validation phase (assumes prior run already produced state)

```bash
ZDU_SKIP_PHASES=discovery,seed,snapshot_t0,upgrade_blocking,inject_traffic_pre,rolling_restart,inject_traffic_dual,upgrade_nonblocking,runtime_migration \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

### Single Suite F TC-504 (writes preserved during sweep)

```bash
smoke-test/venv/bin/python -m tests.zdu --only-tc 504
```

### Cleanup test entities after a run

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2
  WHERE urn LIKE 'urn:li:dashboard:(test,zdu-tc-%)'
     OR urn LIKE 'urn:li:dataset:(urn:li:dataPlatform:test,zdu-tc-%'
     OR urn LIKE 'urn:li:dashboard:(test,zdu-io-pool-%)'
     OR urn LIKE 'urn:li:dashboard:(test,zdu-gap-%)'
     OR urn LIKE 'urn:li:dashboard:(test,zdu-dual-%)'
     OR urn LIKE 'urn:li:dashboard:(test,zdu-rt-%)';"
```

---

## Troubleshooting

| Symptom                                                               | Fix                                                                                                                                                           |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sweep times out at 600s with `MigrateAspects: no mutators registered` | `./gradlew :datahub-upgrade:bootJar` — stale JAR; the auto-bootJar guard handles this on `ZDUTestRunner.__init__` but may be skipped via `ZDU_SKIP_BOOTJAR=1` |
| `401 Unauthorized` on seed                                            | `datahub init --username datahub --password datahub` and set `DATAHUB_GMS_TOKEN`                                                                              |
| `403 Forbidden` from GMS                                              | Token expired — re-run `datahub init`                                                                                                                         |
| `Sweep emits SKIPPED`                                                 | Framework auto-detects + retries with fresh `upgrade_version`                                                                                                 |
| `validation` phase FAILed but only pre-existing TC-020                | Expected on dev stack — see "Known issues" in the Final Audit                                                                                                 |

---

## Phase-by-phase invocation hints

If you want to **only run** a single phase end-to-end (e.g., for debugging), the framework forces all phases to run in order. The way to "isolate" is to skip everything else:

```bash
# Run ONLY upgrade_blocking + validation (opt out of Phase 0/0.5 + skip irrelevant phases)
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
ZDU_SKIP_PHASES=inject_traffic_pre,rolling_restart,inject_traffic_dual,upgrade_nonblocking,runtime_migration \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Run ONLY rolling_restart + validation (CAUTION: this disrupts the dev stack)
ZDU_OLD_IMAGE_TAG=v1.5.0 ZDU_NEW_IMAGE_TAG=v1.6.0 \
ZDU_SKIP_BUILD_IMAGES=1 ZDU_SKIP_PREPARE_OLD_STACK=1 \
ZDU_SKIP_PHASES=upgrade_blocking,inject_traffic_pre,inject_traffic_dual,upgrade_nonblocking,runtime_migration \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

Note: `seed`, `snapshot_t0`, and `discovery` are needed by downstream phases — skipping them often produces SKIP results instead of failures. Phase 0 (`build_images`) and Phase 0.5 (`prepare_old_stack`) are default-on; opt out via `ZDU_SKIP_BUILD_IMAGES=1` / `ZDU_SKIP_PREPARE_OLD_STACK=1` (preferred) or by adding them to `ZDU_SKIP_PHASES`.
