# Community Feedback: dlt Connector

**PR/Version:** `mh--dlt-ingestion-source` branch
**Tested by:** @maggiehays
**Date:** 2026-03-04

---

**Source system:** dlt (local state reader — `~/.dlt/pipelines/`) · Destination: DuckDB
**DataHub environment:** Quickstart (local Docker, GMS v1.0.0rc3)
**Testing mode:** DataHub Quickstart with synthetic test pipeline
**Ingestion run:** 2026-03-04 03:58 · 11 DataFlows · 115 DataJobs · 608 events · 5.21 seconds

---

## Test Scope

**What was tested:**

- dlt DataFlow (pipeline) and DataJob (resource) metadata ingestion
- dlt platform registration in DataHub UI
- Recipe configuration experience
- Installation experience (from branch checkout)

**Explicitly out of scope:**

- **Lineage stitching** — DuckDB DataHub connector not configured; lineage emitted by connector but destination datasets don't exist in DataHub yet. Full end-to-end test pending.
- **Run history** (`include_run_history: false`) — requires destination DB access, opt-in feature
- **dlt UI logo** — known issue: Quickstart Docker image predates this branch. Logo asset (`src/images/dltlogo.svg`) and `constants.ts` import are correctly defined in the branch.
- **Cross-platform lineage** — noted as future test scenario once destination connector is configured

---

## Verification Results

| Scenario                                                   | Status          | Notes                                                                   |
| ---------------------------------------------------------- | --------------- | ----------------------------------------------------------------------- |
| `datahub_test_pipeline` visible in Pipelines browse        | ✅ Pass         | Appeared correctly under Pipelines → dlt                                |
| DataJobs (`users`, `events`) listed under DataFlow         | ✅ Pass         | Both resources listed with correct names                                |
| DataJob metadata accuracy                                  | ✅ Pass         | Name and pipeline attribution correct                                   |
| Internal dlt tables filtered (`_dlt_pipeline_state`, etc.) | ✅ Pass         | Correctly excluded from output                                          |
| Lineage tab absent (lineage disabled in recipe)            | ✅ Pass         | No lineage tab shown                                                    |
| dlt logo in browse path                                    | ❌ Expected     | Quickstart image predates this PR — will resolve on next release        |
| All pre-existing `~/.dlt/pipelines` ingested by default    | ⚠️ Surprising   | 11 pipelines appeared, not just the test one — needs docs clarification |
| Discoverability for new DataHub users                      | ⚠️ Hard to find | Pipelines section not obvious without prior DataHub knowledge           |

---

## Setup Experience

**Installation:** Easier than expected

- No source system credentials required — connector reads local files only
- Branch checkout + `pip install -e .` in the repo venv worked cleanly

**Configuration intuitiveness:** 5/5

- Minimal recipe: just `pipelines_dir` + sink config
- Field names are clear and well-documented in the config class
- The `${ENV_VAR}` pattern wasn't even needed — no secrets to handle

**Time from zero to first ingest:** ~5 minutes

- Includes: creating synthetic DuckDB test pipeline, writing recipe, running ingestion

**Notable strength:** The no-credentials setup is a genuine differentiator. This is the first DataHub connector tested where zero source credentials were required at all. Significantly lowers the bar for community testing and adoption.

---

## Asset Coverage

| Asset type               | Expected          | Found     | Notes                                                    |
| ------------------------ | ----------------- | --------- | -------------------------------------------------------- |
| DataFlow (dlt pipelines) | 1 (test)          | 11 total  | All pipelines in `~/.dlt/pipelines/` ingested by default |
| DataJob (resources)      | 2 (users, events) | 115 total | All resources from all 11 pipelines                      |
| Internal dlt tables      | 0 (filtered)      | 0         | Correctly excluded                                       |

---

## Feature Support Matrix

| Feature                           | Status     | Notes                                          |
| --------------------------------- | ---------- | ---------------------------------------------- |
| DataFlow (pipeline) metadata      | ✅ Working | Name, platform, status                         |
| DataJob (resource) metadata       | ✅ Working | Name, pipeline attribution, properties         |
| Browse path / hierarchy           | ✅ Working | Correct structure in Pipelines section         |
| Outlet lineage to destination     | N/A        | Disabled — DuckDB connector not pre-configured |
| Run history (DataProcessInstance) | N/A        | Opt-in, requires destination DB access         |
| dlt platform icon                 | ❌ Blocked | Not in current Quickstart image                |

---

## Issues Found

### 1. dlt logo missing in browse path

**Severity:** Low (expected, will auto-resolve)
**Feature area:** Platform registration / UI

The dlt platform icon doesn't appear in the browse path or entity pages when running against the current Quickstart Docker image. The logo asset (`datahub-web-react/src/images/dltlogo.svg`) exists and is correctly imported in `constants.ts` on this branch. The issue is that the running Quickstart was built before this PR.

**Resolution:** Will resolve automatically when the next DataHub release ships with this branch's frontend changes. No code change needed.

---

### 2. All `~/.dlt/pipelines/` ingested by default — no obvious scoping

**Severity:** Medium
**Feature area:** Default configuration / documentation

The connector scanned 11 pre-existing pipelines beyond the synthetic test pipeline. For a user running this against a machine with many dlt pipelines, this could be a lot of noise or unexpected ingestion.

**Expected behavior:** Users should be clearly told that `pipeline_pattern` defaults to allow-all, and the docs should include an example of how to scope ingestion to specific pipelines.

**Suggested fix:** Add a `pipeline_pattern` example to the recipe documentation:

```yaml
source:
  config:
    pipeline_pattern:
      allow:
        - "^prod_.*" # Only ingest production pipelines
```

---

### 3. Discoverability of dlt entities in DataHub UI

**Severity:** Medium (Enhancement)
**Feature area:** Documentation / onboarding

New DataHub users don't naturally navigate to the Pipelines section to find dlt entities. During testing, finding `datahub_test_pipeline` required knowing about the Pipelines tab and its structure.

**Suggested fix:** Connector docs should include:

- Explicit navigation path: Pipelines → [dlt icon] → [pipeline name]
- A screenshot or description of what a DataFlow/DataJob looks like in the DataHub UI
- A note that dlt pipelines appear under "Pipelines" not "Datasets"

---

### 4. Lineage stitching setup not documented

**Severity:** Medium (Documentation gap)
**Feature area:** Lineage / cross-platform

The recipe config has `include_lineage: true` by default, but the docs don't explain that for lineage to appear end-to-end in DataHub, the destination platform connector (e.g., a DuckDB or Postgres connector) must have already ingested the destination tables.

Without that prerequisite, lineage arrows are emitted but point to datasets that don't exist yet in DataHub, creating dangling references.

**Suggested fix:** Add a "Prerequisites for lineage" section to the docs:

> For outlet lineage to stitch end-to-end in DataHub, you must first ingest the
> destination platform using its DataHub connector. For example, if your dlt pipeline
> loads to Postgres, run the Postgres DataHub connector before (or after) this connector.

---

## Overall Assessment

**Verdict:** ⚠️ Ready with minor polish

**Would use in production:** Need to test against real production dlt pipelines first (currently tested with synthetic DuckDB pipeline only)

**Biggest strength:** Zero-credential setup. The connector reads local files — no source system access, no role setup, no connection strings. Lowest-friction DataHub connector tested so far.

**Top improvement needed:** Documentation — specifically around:

1. Default `pipeline_pattern: allow-all` and how to scope it
2. UI navigation for finding dlt entities
3. Lineage prerequisites

**Positive surprises:**

- Ingestion was fast (~5s for 11 pipelines / 115 resources)
- Internal dlt system tables (`_dlt_pipeline_state`, `_dlt_loads`) correctly filtered out automatically
- Config class field documentation is thorough and helpful

**Security / compliance concerns:** None. The no-credentials design is actually a security positive.

---

## Cross-Platform Lineage Coverage

| Scenario                      | In scope for this PR | Tested        | Result                              |
| ----------------------------- | -------------------- | ------------- | ----------------------------------- |
| dlt → DuckDB outlet lineage   | ✅ Yes               | ❌ Not tested | DuckDB connector not pre-configured |
| dlt → Postgres outlet lineage | ✅ Yes               | N/A           | Not in test environment             |

**Out-of-scope lineage use cases (documented for connector roadmap):**

- End-to-end lineage with destination connector requires pre-ingesting destination platform — not yet validated

---

_Community feedback generated with the DataHub Connector Community Feedback skill._
_No credentials were shared in this session. Ingestion run by agent using local-file-only connector (no source credentials required)._
