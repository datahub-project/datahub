# dlt Connector - Planning Document

**Created**: 2026-03-04
**Status**: IN_PROGRESS

---

## Overview

dlt (data load tool) is an open-source Python ELT library for building data pipelines. This connector reads pipeline metadata from dlt's local state directory (`~/.dlt/pipelines/`) and emits DataFlow, DataJob, and lineage entities to DataHub.

The connector has **substantial existing implementation** but was built for a local demo environment. This document captures what exists, what needs fixing, and what needs to be built — with the goal of making it production-quality and executable via `datahub quickstart` for any open-source user.

---

## Current State Assessment

### What EXISTS (already implemented)

| File                                            | Status         | Notes                                                           |
| ----------------------------------------------- | -------------- | --------------------------------------------------------------- |
| `src/.../dlt/dlt.py`                            | ✅ Implemented | DltSource — StatefulIngestionSourceBase + TestableSource        |
| `src/.../dlt/config.py`                         | ✅ Implemented | DltSourceConfig, DestinationPlatformConfig, DltRunHistoryConfig |
| `src/.../dlt/dlt_client.py`                     | ✅ Implemented | SDK + filesystem YAML fallback                                  |
| `src/.../dlt/data_classes.py`                   | ✅ Implemented | Data classes — but contains orphaned dead code (see below)      |
| `src/.../dlt/dlt_report.py`                     | ✅ Implemented | DltSourceReport — has one method never called                   |
| `tests/unit/dlt/test_dlt_source.py`             | ✅ Complete    | 10 solid unit tests                                             |
| `tests/integration/dlt/docker-compose.yml`      | ✅ Exists      | Postgres test DB on port 5433                                   |
| `tests/integration/dlt/setup_chess_pipeline.py` | ✅ Exists      | Chess.com pipeline setup fixture                                |
| `tests/integration/dlt/dlt_to_file.yml`         | ✅ Exists      | dlt→file recipe                                                 |
| `tests/integration/dlt/postgres_to_file.yml`    | ✅ Exists      | postgres→file recipe                                            |

### What's MISSING (gaps to fill)

| Gap                                                   | Impact                                                                 | Priority |
| ----------------------------------------------------- | ---------------------------------------------------------------------- | -------- |
| `setup.py` entry point + plugin registration          | **BLOCKER** — `datahub ingest -c recipe.yml` silently fails without it | P0       |
| `tests/integration/dlt/test_dlt.py`                   | No integration test runs — infrastructure exists but no test file      | P1       |
| Platform registration in `data-platforms.yaml` + logo | dlt has no icon or display name in DataHub UI                          | P1       |
| `docs/sources/dlt/dlt.md` + `dlt_recipe.yml`          | No documentation for open-source users                                 | P2       |

### Bugs and Demo Artifacts to Fix

These were identified by close code review. Several are demo artifacts from the local setup; others are genuine bugs.

#### 1. Demo artifact — chess examples in config field description

**File**: `config.py`, `source_table_dataset_urns` field description

The description uses chess-specific examples (`chess_sync`, `players_games`, `players_profiles`). These are tied to the local demo and will confuse open-source users. Fix: replace with generic pipeline/table names.

#### 2. Dead code — `DltTraceInfo` never used

**File**: `data_classes.py`

`DltTraceInfo` (with `last_extract_info`, `last_normalize_info`, `last_load_info`, `row_counts`) is defined but is never imported or referenced anywhere in the connector. It appears to be a prototype from the demo that was never wired up. Remove it.

#### 3. Bug — duplicate `import json` inside function body

**File**: `dlt_client.py:261`

`import json` appears inside `_get_pipeline_info_from_filesystem()`, duplicating the module-level import on line 5. Remove the inner import.

#### 4. Bug — `_build_datajob` hardcodes `schemas[0].schema_name`

**File**: `dlt.py:227`

```python
"schema_name": pipeline_info.schemas[0].schema_name if pipeline_info.schemas else "",
```

The outer loop iterates `for schema in pipeline_info.schemas: for table in schema.tables` — so the loop already has the correct schema in scope. Using `schemas[0]` is wrong for multi-schema pipelines (a pipeline with two dlt sources produces two schemas). Fix: thread the current `schema` object into `_build_datajob` and use `schema.schema_name` directly.

#### 5. Incomplete error handling — `report_run_history_error()` never called

**File**: `dlt_report.py:55`, `dlt.py._emit_run_history`

`DltSourceReport.report_run_history_error()` is defined but never invoked. When `client.get_run_history()` fails, it returns an empty list silently and the report never reflects the failure. The error should surface. Fix: call `self.report.report_run_history_error()` from `_emit_run_history` when the client returns empty for an unexpected reason, or restructure so errors propagate to the report.

---

## Research Summary

### Source Classification

- **Type**: Orchestration Tool (ELT pipeline framework)
- **Source Category**: orchestration_tools
- **Interface**: Python SDK (`dlt`) + local filesystem YAML/JSON state files
- **Standards File**: `standards/source_types/orchestration_tools.md`
- **Documentation**: https://dlthub.com/docs/general-usage/pipeline

### Similar DataHub Connectors

| Connector          | Relevance | Key Patterns Used                                                                                                      |
| ------------------ | --------- | ---------------------------------------------------------------------------------------------------------------------- |
| fivetran           | High      | Same DataFlow/DataJob/DataProcessInstance pattern; StatefulIngestionSourceBase; SDK V2; destination log table querying |
| dbt                | High      | Filesystem-first metadata reading; reads local artifact files; no live connection needed                               |
| azure_data_factory | Medium    | DataFlow/DataJob entity hierarchy                                                                                      |

---

## Entity Mapping

| dlt Concept                                | DataHub Entity      | URN Format                                                 | Notes                                                              |
| ------------------------------------------ | ------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------ |
| `pipeline_name` (per `dlt.pipeline()`)     | DataFlow            | `urn:li:dataFlow:(dlt,<pipeline_name>,<env>)`              | One DataFlow per pipeline                                          |
| Table/resource (per `schema.tables` entry) | DataJob             | `urn:li:dataJob:(urn:li:dataFlow:...,<table_name>)`        | One DataJob per destination table                                  |
| Destination table                          | Dataset (outlet)    | `urn:li:dataset:(urn:li:dataPlatform:<dest>,<path>,<env>)` | Outlet lineage — must match the destination connector's URN format |
| Source table (sql_database pipelines)      | Dataset (inlet)     | `urn:li:dataset:(urn:li:dataPlatform:<src>,<path>,<env>)`  | User-configured only — dlt does not store source identity          |
| `_dlt_loads` row                           | DataProcessInstance | `urn:li:dataProcessInstance:<pipeline>_<load_id>`          | Opt-in run history                                                 |

**Key limitation**: dlt's state files record the _destination_ but not the _source system_ connection. Inlet Dataset URNs for lineage must be manually configured by the user via `source_dataset_urns` (pipeline-level) or `source_table_dataset_urns` (table-level). This is fundamental to dlt's architecture, not a connector design choice.

---

## Architecture Decisions

### Base Class

**Chosen**: `StatefulIngestionSourceBase` + `TestableSource` — already implemented, correct.

dlt is not a SQL source. No SQLAlchemy dialect exists. The connector reads from the local filesystem with no live API or network calls. `StatefulIngestionSourceBase` provides stale entity removal when pipelines are deleted.

### Client Design — Dual-Path Filesystem + SDK

**Chosen**: `DltClient` with two read paths, already implemented.

1. **Primary (dlt SDK)**: `dlt.pipeline(pipeline_name=..., pipelines_dir=...)` attaches to an existing pipeline without executing it, giving access to `pipeline.schemas`, `pipeline.destination`, `pipeline.dataset_name`. dlt SDK must be installed.
2. **Fallback (filesystem)**: Parses `<pipelines_dir>/<pipeline_name>/schemas/*.schema.yaml` and `state.json` directly. Works with no dlt installation. This is the path most DataHub users will take.

The fallback makes the connector viable for any DataHub deployment — the dlt SDK does not need to be present where DataHub ingestion runs.

### Run History Design — Current Approach and Future Direction

**Current approach**: `DltClient.get_run_history()` uses `pipeline.sql_client()` to query `_dlt_loads` from the destination. This requires the dlt SDK to be installed and destination credentials in `~/.dlt/secrets.toml`.

**Comparison with fivetran**: Fivetran's DataHub connector connects _directly_ to the destination (Snowflake, BigQuery, Databricks) using an explicit `FivetranLogConfig` with typed connection credentials per platform. It does not depend on the fivetran SDK at all — it uses DataHub's own SQLAlchemy-based connection configs. This is more robust and portable.

**The dlt case is different in one important way**: fivetran writes logs to a dedicated `fivetran_log` schema separate from user data. dlt's `_dlt_loads` lives _in the same dataset as the loaded data_ (e.g. `chess_data._dlt_loads`). This makes direct destination access simpler — the user's destination credentials already point at the right place.

**Decision for v1**: Keep the current dlt SDK approach (`pipeline.sql_client()`). It works for the quickstart scenario (dlt and DataHub on the same machine) and is gated behind `include_run_history: false` by default. This is an acceptable starting point.

**Future work (v2)**: Add an optional `destination_connection` sub-config modeled after fivetran's `FivetranLogConfig`, allowing users to provide explicit destination credentials without installing dlt. This enables run history from any machine with network access to the destination. Skeleton design:

```python
class DltDestinationConnectionConfig(ConfigModel):
    """
    Optional direct connection to the dlt destination to query _dlt_loads.
    When provided, run history does not require the dlt package.
    """
    platform: Literal["postgres", "bigquery", "snowflake", "duckdb", "redshift"]
    # Reuses existing tested connection config classes already in DataHub:
    postgres_config: Optional[PostgresConnectionConfig] = None
    bigquery_config: Optional[BigQueryConnectionConfig] = None
    snowflake_config: Optional[SnowflakeConnectionConfig] = None
    dataset_name: str  # the dlt dataset_name (schema) where _dlt_loads resides
```

### `pipelines_dir` and Open-Source Deployment Scenarios

`~/.dlt/pipelines` is dlt's actual default — any user who runs `dlt.pipeline().run()` without customizing `pipelines_dir` will have state here. The config default is correct.

However, the documentation must explicitly address these three scenarios that open-source users will encounter:

**Scenario 1 — Same machine (local / quickstart)**

```
dlt runs on the same machine as DataHub ingestion.
pipelines_dir: "~/.dlt/pipelines"  # works out of the box
```

**Scenario 2 — CI/CD (GitHub Actions, Airflow, etc.)**

```
dlt pipeline runs in one job → writes state to a path or mounted volume.
DataHub ingestion runs in a separate job → must point at the same path.
pipelines_dir: "/data/dlt-pipelines"  # or whatever shared path/mount
```

Many dlt users already set a `PIPELINES_DIR` env var. The recipe can reference it:

```yaml
pipelines_dir: "${PIPELINES_DIR:-~/.dlt/pipelines}"
```

**Scenario 3 — Kubernetes / containerized**

```
dlt runs in one pod with a PersistentVolumeClaim.
DataHub ingestion runs separately and must mount the same PVC.
pipelines_dir: "/mnt/dlt-pipelines"  # the PVC mount path
```

The documentation must cover all three scenarios with concrete examples. This is the #1 confusion point for new users.

### Config Design

Inherits from: `StatefulIngestionConfigBase` + `PlatformInstanceConfigMixin` + `EnvConfigMixin`

```yaml
source:
  type: dlt
  config:
    # Path where dlt stores pipeline state.
    # Default (~/.dlt/pipelines) works for local/quickstart use.
    # Override for CI, Docker, or Kubernetes deployments.
    pipelines_dir: "~/.dlt/pipelines"

    # Filter pipelines by name (regex)
    pipeline_pattern:
      allow: [".*"]

    # Emit outlet lineage from DataJobs to destination Dataset URNs
    include_lineage: true

    # Per-destination URN construction (must match your destination connector's config)
    destination_platform_map:
      postgres:
        database: my_database # needed for 3-part URN: database.schema.table
        platform_instance: null
        env: PROD
      bigquery:
        platform_instance: my-gcp-project
        env: PROD

    # Optional: manually specify inlet (upstream) Dataset URNs
    # dlt does not record source connection info, so these must be provided by the user.
    # Use source_dataset_urns for pipeline-level inlets (e.g. REST API sources)
    source_dataset_urns:
      my_pipeline:
        - "urn:li:dataset:(urn:li:dataPlatform:salesforce,contacts,PROD)"

    # Use source_table_dataset_urns for table-level inlets (e.g. sql_database sources)
    source_table_dataset_urns:
      my_pipeline:
        my_table:
          - "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)"

    # Run history: opt-in, requires dlt package + destination credentials
    include_run_history: false
    run_history_config:
      start_time: "-7 days"

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

    env: PROD
```

---

## Capabilities

| Capability           | Status           | Notes                                                                                            |
| -------------------- | ---------------- | ------------------------------------------------------------------------------------------------ |
| `PLATFORM_INSTANCE`  | ✅ Supported     | Declared and implemented                                                                         |
| `LINEAGE_COARSE`     | ✅ Supported     | Outlet: auto-constructed from dlt state. Inlet: user-configured only                             |
| `LINEAGE_FINE`       | ✅ Partial       | Column-level lineage emitted only for single-inlet / single-outlet tables (unambiguous 1:1 copy) |
| `DELETION_DETECTION` | ✅ Supported     | Via stateful ingestion                                                                           |
| `OWNERSHIP`          | ❌ Not supported | dlt state does not contain owner information; declared `supported=False`                         |
| `DATA_FLOW`          | ✅ Implicit      | DataFlow emitted per pipeline                                                                    |
| `DATA_JOB`           | ✅ Implicit      | DataJob emitted per destination table                                                            |

---

## Testing Strategy

### Unit Tests — COMPLETE

`tests/unit/dlt/test_dlt_source.py` — 10 tests covering all core behaviors. No changes needed (bugs in Phase 2 will require updating 1-2 tests that exercise the multi-schema path).

### Integration Tests — INFRASTRUCTURE EXISTS, TEST FILE MISSING

**Infrastructure ready:**

- `docker-compose.yml`: `postgres:16-alpine` on port 5433 (database `chess`, credentials `dlt/dlt`)
- `setup_chess_pipeline.py`: Runs Chess.com dlt pipeline (MagnusCarlsen + Hikaru, Jan–Feb 2024). Deterministic and reproducible.
- `dlt_to_file.yml`: Recipe for dlt connector → file sink
- `postgres_to_file.yml`: Recipe for postgres connector → file sink (lineage stitching validation)

**What needs to be created**: `tests/integration/dlt/test_dlt.py` + golden files.

**Test file structure**:

```python
@pytest.mark.integration
class TestDltIntegration:

    def test_dlt_connector_golden(tmp_path, pytestconfig):
        """Full dlt connector output matches golden file."""
        # 1. docker-compose up (postgres)
        # 2. Run setup_chess_pipeline.py → creates real state in tmp_path
        # 3. Run datahub ingest -c dlt_to_file.yml
        # 4. Compare output to tests/integration/dlt/golden/dlt_golden.json

    def test_postgres_destination_golden(tmp_path):
        """Postgres connector output matches golden file (for lineage stitching)."""
        # Verifies that dlt outlet URNs match postgres connector Dataset URNs

    def test_lineage_stitching(dlt_output, postgres_output):
        """At least one dlt DataJob outlet URN appears as a Dataset URN in postgres output."""
```

**Golden file requirements:**

- ≥1 DataFlow (`chess_pipeline`)
- ≥4 DataJob entities (players_games, players_profiles, players_archives, players_online_status)
- ≥3 nested child table DataJobs (e.g. `players_games__opening`)
- Outlet lineage edges pointing to postgres Dataset URNs
- Lineage stitching: at least one dlt outlet URN must match a Dataset URN from the postgres connector golden file

---

## Known Limitations

| Limitation                                                 | Impact                                                                            | Workaround                                                                            |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Inlet URNs cannot be auto-detected                         | Users must manually configure `source_dataset_urns` / `source_table_dataset_urns` | Clear docs + examples for REST API vs sql_database pipelines                          |
| Run history requires dlt SDK + destination credentials     | `include_run_history: false` by default                                           | Opt-in; v2 will add direct destination connection config to remove dlt SDK dependency |
| `pipelines_dir` must be accessible from where DataHub runs | Breaks for cross-machine or container deployments without shared storage          | Document three deployment scenarios; users must mount/share the directory             |
| Column-level lineage only for 1-inlet / 1-outlet tables    | REST API pipelines get no CLL                                                     | Documented limitation; CLL requires unambiguous 1:1 column mapping                    |
| Multi-schema pipelines (v1 bug)                            | `schema_name` in DataJob custom props is always first schema's name               | Fixed in Phase 2                                                                      |

---

## Implementation Phases

### Phase 1 — Registration (P0 — BLOCKER)

Enables `datahub ingest -c recipe.yml` to work.

- [ ] Add `dlt` plugin to `setup.py` `plugins` dict with minimal optional dependency:
  ```python
  dlt_plugin = {"dlt>=0.4.0"}  # optional; connector works without it via filesystem fallback
  plugins["dlt"] = dlt_plugin
  ```
- [ ] Add entry point in `setup.py`:
  ```python
  "dlt = datahub.ingestion.source.dlt.dlt:DltSource",
  ```
- [ ] Run `./gradlew :metadata-ingestion:lintFix` and verify no regressions

### Phase 2 — Code Quality and Demo Artifact Cleanup (P1)

Fixes bugs and removes demo-specific artifacts.

- [ ] **`config.py`**: Replace chess-specific examples in `source_table_dataset_urns` field description with generic `my_pipeline` / `my_table` examples
- [ ] **`data_classes.py`**: Remove `DltTraceInfo` dataclass (dead code, never imported or used)
- [ ] **`dlt_client.py`**: Remove duplicate `import json` inside `_get_pipeline_info_from_filesystem` function body
- [ ] **`dlt.py`**: Fix `_build_datajob` to accept `schema_name: str` as a parameter (passed in from the outer `schema` loop) instead of hardcoding `schemas[0].schema_name`
- [ ] **`dlt.py` + `dlt_report.py`**: Wire up `report_run_history_error()` — call it when `get_run_history` returns empty unexpectedly, or expose the failure reason in the report

### Phase 3 — Integration Test (P1)

- [ ] Create `tests/integration/dlt/test_dlt.py`
- [ ] Generate golden files by running the Chess.com pipeline against the test Postgres
- [ ] Add lineage stitching test: verify at least one dlt outlet URN matches a postgres Dataset URN
- [ ] Add `conftest.py` fixtures for docker-compose lifecycle and pipeline setup

### Phase 4 — Platform Registration (P1)

- [ ] Add `dlt` entry to `metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml`:
  ```yaml
  - entityUrn: urn:li:dataPlatform:dlt
    entityType: dataPlatform
    aspectName: dataPlatformInfo
    changeType: UPSERT
    aspect:
      datasetNameDelimiter: "."
      name: dlt
      displayName: dlt
      type: OTHERS
      logoUrl: "assets/platforms/dltlogo.png"
  ```
- [ ] Source official dlt logo (transparent PNG, ≥128×128, from https://dlthub.com or the dlt GitHub repo)
- [ ] Add logo to `datahub-web-react/public/assets/platforms/dltlogo.png`

### Phase 5 — Documentation (P2)

- [ ] Create `docs/sources/dlt/dlt.md`:
  - Overview and entity mapping
  - **"Where to find your `pipelines_dir`"** section covering local, CI/CD, and Kubernetes deployment scenarios
  - Required config fields and what each does
  - Lineage stitching guide (how `destination_platform_map` + `source_dataset_urns` work together)
  - Run history prerequisites and setup
  - Troubleshooting (empty output, lineage not stitching)
- [ ] Create `docs/sources/dlt/dlt_recipe.yml` — fully commented reference recipe with all options and generic examples (no chess references)

### Future Work (Post-v1)

- [ ] Add `DltDestinationConnectionConfig` to `DltRunHistoryConfig`: explicit destination credentials (postgres, bigquery, snowflake) so run history does not require dlt SDK. Model after fivetran's `FivetranLogConfig`. Enables run history from any DataHub deployment without dlt installed.

---

## Local Testing Quickstart

End-to-end test against a local DataHub:

```bash
# 1. Install dev environment and dlt
cd metadata-ingestion
../gradlew :metadata-ingestion:installDev
source venv/bin/activate
pip install "dlt[postgres]"

# 2. Install dlt Chess.com verified source (run once)
dlt init chess postgres

# 3. Start test Postgres (dlt destination)
cd tests/integration/dlt
docker-compose up -d

# 4. Run the dlt pipeline to generate local state files
DLT_PIPELINES_DIR=/tmp/dlt-test python setup_chess_pipeline.py

# 5. Inspect output via file sink
DLT_PIPELINES_DIR=/tmp/dlt-test OUTPUT_FILE=/tmp/dlt-output.json \
  datahub ingest -c dlt_to_file.yml

cat /tmp/dlt-output.json | python -m json.tool | head -200

# 6. For live DataHub quickstart:
# Start DataHub: cd <repo-root> && ./gradlew quickstartDebug
# Then run with datahub-rest sink pointing at localhost:8080
```

---

## Approval

- [x] User approved this plan on: 2026-03-04
- [x] Approval message: "yep, update planning"
