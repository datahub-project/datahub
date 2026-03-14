# dlt Connector — Full Review Report (v2)

**Branch:** `mh--dlt-ingestion-source`
**Reviewer:** datahub-skills:datahub-connector-pr-review (automated)
**Date:** 2026-03-04
**Previous review:** `_datahub-connector-pr-review-2026-03-04.md`
**Verdict: NEEDS CHANGES** — 4 new/escalated blockers. Previously resolved: 10/16 issues fixed.

---

## What Changed Since v1

### Resolved (10 items)

| Previous ID                                             | Fix                                                                                                                                                                                                                                              |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| B1 — silent state.json exception                        | `logger.warning(..., pipeline_name, e)` added at `dlt_client.py:260`                                                                                                                                                                             |
| B2 — run history failure not surfaced                   | `else` branch added at `dlt.py:385`; both failure modes now call `report.warning()` + `report_run_history_error()`                                                                                                                               |
| B3 — trivial status mapping test                        | Replaced with `test_run_history_emits_dataprocess_instance` that mocks `get_run_history` and verifies DPI entity type + `run_history_loaded == 2`                                                                                                |
| B4 — no test for `include_lineage=False`                | `test_dlt_include_lineage_false` added                                                                                                                                                                                                           |
| B5 — DRY: duplicated column parsing                     | `_parse_columns()` and `_parse_table()` extracted as module-level helpers; both `_parse_schema_file` and `_schema_obj_to_info` now use them                                                                                                      |
| W1 — empty schemas not surfaced                         | `report.warning()` added at `dlt.py:162` when `pipeline_info.schemas` is empty                                                                                                                                                                   |
| W3 — `DltRunHistoryConfig.enabled` redundant gate       | Removed; `include_run_history` is the sole gate                                                                                                                                                                                                  |
| W4 — dead `table_outlet_urns` variable                  | Removed                                                                                                                                                                                                                                          |
| W5 — no CLL unit test                                   | `test_column_level_lineage_emitted_for_single_inlet_outlet` added                                                                                                                                                                                |
| W6 — no test for missing destination_platform_map entry | `test_dlt_outlet_urns_fallback_when_destination_not_in_map` added                                                                                                                                                                                |
| S1–S8                                                   | Dead `TYPE_CHECKING` removed; `Any` justified in docstring; unused property removed; `dlt_available` public property added; `_DLT_LOAD_STATUS_MAP` module constant; list comprehension in `list_pipeline_names`; `Dict` annotations standardized |

---

## Summary

| Category          | Status          | Notes                                                          |
| ----------------- | --------------- | -------------------------------------------------------------- |
| Architecture      | ✅ PASS         | Correct base class, SDK V2 used throughout                     |
| Code Organization | ✅ PASS         | Clean file structure, proper separation                        |
| Error Handling    | 🔴 NEEDS FIXES  | 1 blocker, 3 warnings — DltClient still lacks report reference |
| Code Complexity   | 🔴 NEEDS FIXES  | 2 blockers — `_emit_run_history` too long, unused parameter    |
| Type Safety       | 🟡 MINOR ISSUES | Dict/dict inconsistency, Literal gap in data_classes           |
| Test Quality      | 🔴 NEEDS FIXES  | 2 blockers — CLL and DPI not tested at integration level       |
| Performance       | ✅ PASS         | Generators throughout, no N+1 patterns                         |
| Security          | ✅ PASS         | No hardcoded secrets, no injection risk                        |
| Documentation     | ✅ PASS         | All config fields documented                                   |

---

## 🔴 BLOCKERS (Must Fix)

### B1 — `_emit_run_history` is 68 lines — exceeds 50-line standard

**`dlt.py:370–437`**

The function does two distinct things: (1) fetch loads and warn/early-return (lines 374–393), and (2) per-load DPI construction and emission (lines 395–436). The per-load block alone is 40 lines of inline V1 object construction and three separate MCP generator loops. This directly violates the `<50 lines` standard and was flagged as S6 in the previous review but not acted on.

**Fix:** Extract `_emit_dpi_for_load(self, pipeline_info, load) -> Iterable[MetadataWorkUnit]` for the per-load block. The coordinator function drops to ~15 lines.

---

### B2 — `dataflow: DataFlow` parameter passed to `_emit_run_history` but never used

**`dlt.py:370–371, 400–405`**

The `dataflow: DataFlow` parameter is declared and accepted but never referenced anywhere in the function body. Instead, a `DataFlowUrn` is rebuilt from scratch using `pipeline_info` and `self.config` — duplicating information already encoded in the `dataflow` object that was passed in. This is both a dead parameter and a DRY violation: the two URN constructions must always stay in sync, but nothing enforces this.

**Fix:** Either remove the `dataflow` parameter and keep the manual reconstruction, or derive the URN from the `dataflow` object directly.

---

### B3 — `fineGrainedLineages` is `[]` in every golden file record — CLL untested at integration level

**`tests/integration/dlt/golden/dlt_golden.json` — all `dataJobInputOutput` records**

The unit test `test_column_level_lineage_emitted_for_single_inlet_outlet` covers CLL at the unit level. However, the integration golden file is generated without any `source_table_dataset_urns`, so all 5 `dataJobInputOutput` aspects show `"fineGrainedLineages": []`. A regression that silently drops CLL from serialization (e.g., a bug in `DataJob.as_workunits()` ignoring the `fine_grained_lineages` parameter) would not be caught.

**Fix:** Add an integration test variant that configures `source_table_dataset_urns` for `chess_pipeline.players_games` → inlet URN and asserts non-empty `fineGrainedLineages` in the output.

---

### B4 — `DataProcessInstance` / run history never tested at integration level

**`tests/integration/dlt/test_dlt.py` — no test covers `include_run_history=True`**

The run history path (`_emit_run_history` → `DataProcessInstance`) is exercised only by unit test 6, which mocks `get_run_history`. No integration test sets `include_run_history: true`, configures `source_table_dataset_urns`, and verifies that `dataProcessInstance` entities appear in the serialized output with the correct structure.

**Fix:** Add an integration test with `include_run_history=True` and a mocked (or real) `get_run_history` return, asserting `dataProcessInstance` entity type in output and `run_history_loaded` count.

---

## 🟡 WARNINGS (Should Fix)

### W1 — All five `logger.warning()` calls in `dlt_client.py` use f-strings

**`dlt_client.py:85, 204, 231, 244, 341`**

Python logging best practice — and the DataHub project convention — requires `%s`-style lazy formatting so strings are not constructed when the log level is filtered. All five calls in `dlt_client.py` use f-strings. The `state.json` fix at line 260–265 correctly uses `%s` format; the others do not.

**Fix:**

```python
# Line 85
logger.warning("Failed to read schema file %s: %s", schema_path, e)
# Line 204
logger.warning("dlt SDK failed for pipeline '%s', falling back to filesystem: %s", pipeline_name, e)
# Line 231
logger.warning("Could not convert schema '%s' from SDK: %s", schema_name, e)
# Line 244
logger.warning("No schemas found for pipeline '%s'", pipeline_name)
# Line 341
logger.warning("Failed to query _dlt_loads for pipeline '%s': %s", pipeline_name, e)
```

---

### W2 — `DltClient` has no report reference — SDK schema failures invisible in ingestion report

**`dlt_client.py` — `_parse_schema_file`, `_schema_obj_to_info`, `_read_schemas_from_filesystem`**

This was W2 in the previous review and was not fixed. `DltClient` has no access to `DltSourceReport`, so when `_parse_schema_file` or `_schema_obj_to_info` fail silently and return `None`, the call sites in `_read_schemas_from_filesystem` and `_get_pipeline_info_via_sdk` drop the schema without incrementing `schema_read_errors` or calling `report.warning()`. Users see no indication in the DataHub UI that any schemas failed to parse.

The `state.json` parse failure at line 260 (B1 from previous review) was fixed with logging but still has no `report` path — it is correctly flagged as W6 below.

**Fix (architectural):** Inject `DltSourceReport` into `DltClient.__init__` and call `self.report.warning(...)` / `self.report.report_schema_read_error()` directly from the failing methods. This matches how other DataHub connectors structure their clients.

---

### W3 — `test_connection` reports `capable=True` with non-empty `failure_reason` — contradictory state

**`dlt.py:479–487`**

```python
report.capability_report[SourceCapability.LINEAGE_COARSE] = CapabilityReport(
    capable=True,
    failure_reason=(
        "dlt package not installed — will use filesystem YAML fallback. "
        "Install with: pip install dlt"
    ),
)
```

`capable=True` with a non-empty `failure_reason` is a contradictory state that will confuse users in the DataHub UI. The filesystem fallback works without dlt installed, so this branch should be either `capable=True` with no failure reason (lineage works via filesystem) or reframed as an informational message via a different mechanism.

**Fix:** If filesystem fallback provides lineage, set `capable=True` with no `failure_reason`. Add a separate informational field if needed.

---

### W4 — Missing `state.json` produces `destination=""` silently with no warning

**`dlt_client.py:247–267`**

The previous B1 fix handles the case where `state.json` exists but is corrupt. The case where `state.json` is absent entirely (a partially initialized pipeline directory) is still unhandled — `destination` and `dataset_name` silently default to `""` with no log or report entry. A pipeline with no `state.json` will emit DataJobs with malformed outlet URNs.

**Fix:** Add a `logger.warning(...)` call (and ideally a `report.warning()` path) when `state_file.exists()` is `False`.

---

### W5 — `test_dlt_include_lineage_false` has a vacuous loop

**`tests/integration/dlt/test_dlt.py:274–298`**

The test only asserts `output_datasets == []` inside a `for` loop over all records. If the pipeline crashed before emitting any entities, the loop body would never execute and the test would pass vacuously. No guard assertion verifies that DataFlow/DataJob entities are present.

**Fix:** Add `assert any(r.get("entityType") == "dataJob" for r in records), "Expected DataJobs to be present"` before the loop.

---

### W6 — DataFlow entity missing `browsePathsV2` aspect in golden file

**`tests/integration/dlt/golden/dlt_golden.json`**

All 5 DataJob entities emit `browsePathsV2` correctly. The DataFlow entity emits only `dataPlatformInstance`, `dataFlowInfo`, and `status`. The absence of `browsePathsV2` on the DataFlow may indicate a real gap in `_build_dataflow` output or may be intentional — either way it is undocumented and untested.

**Fix:** Verify whether `DataFlow.as_workunits()` should emit `browsePathsV2`. If it should, add the aspect; if not, add a comment explaining why it is absent.

---

## ℹ️ SUGGESTIONS (Optional)

| #   | Location                                             | Description                                                                                                                                                                    |
| --- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| S1  | `dlt_client.py:156, 306`                             | Internal methods use `self._dlt_available` directly; should use `self.dlt_available` for consistency with the property boundary                                                |
| S2  | `dlt_client.py:321–335`                              | Index-based SQL row access (`row[0]`, `row[1]`…) fragile if query column order changes; named access preferred                                                                 |
| S3  | `dlt.py:72–74`                                       | `_DLT_LOAD_STATUS_MAP` maps only `0`; add a comment documenting that `status=1` means in-progress (treated as FAILURE)                                                         |
| S4  | `data_classes.py:27, 38`                             | `write_disposition: str` and `data_type: str` could be `Literal["append", "replace", "merge"]` and `Literal["text", "bigint", ...]` respectively for stronger type enforcement |
| S5  | `tests/unit/dlt/test_dlt_source.py:33`               | `FROZEN_TIME` constant defined but no unit test applies `@time_machine.travel(FROZEN_TIME)` — dead constant                                                                    |
| S6  | `tests/unit/dlt/test_dlt_source.py`                  | `source_dataset_urns` pipeline-level inlet config path has no unit test                                                                                                        |
| S7  | `tests/unit/dlt/test_dlt_source.py`                  | Invalid inlet URN warning path (`_build_inlet_urns` exception handler at `dlt.py:320–326`) has no test                                                                         |
| S8  | `dlt.py:479–487`                                     | `str(e)` in outer `test_connection` except can produce empty string on some exception types; prefer `repr(e)`                                                                  |
| S9  | `tests/integration/dlt/golden/dlt_golden.json:70–71` | Golden file still contains absolute `/Users/maggiehayes/...` paths; `ignore_paths` prevents CI failures but committed values are machine-specific                              |
| S10 | `tests/integration/dlt/test_dlt.py:157–165`          | `extra_config` in `test_dlt_outlet_urns_match_postgres_format` is identical to the defaults in `_run_dlt_pipeline` — override is a no-op                                       |

---

## What's Working Well

- **All 5 previous blockers fixed** — B1 through B5 resolved cleanly
- **Test count grew from 15 to 18** — 3 new meaningful tests added (CLL, fallback URN, `include_lineage=False`)
- **DRY violation eliminated** — `_parse_columns` / `_parse_table` design is clean and correct
- **SDK V2 adoption** — `DataFlow` and `DataJob` from `datahub.sdk` throughout; all required aspects auto-emit correctly
- **`_DLT_LOAD_STATUS_MAP` constant** — cleaner than per-call dict
- **`dlt_available` public property** — correct implementation and correctly used externally
- **`list_pipeline_names` generator** — cleaner than nested-if append
- **18/18 tests passing** — unit + integration, all clean on lint
- **End-to-end validated** — 30 events, 0 failures, 0 warnings against live DataHub quickstart
