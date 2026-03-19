# dlt Connector — Full Review Report

**Branch:** `mh--dlt-ingestion-source`
**Reviewer:** datahub-skills:datahub-connector-pr-review (automated)
**Date:** 2026-03-04
**Verdict: NEEDS CHANGES** — 5 blockers require fixes before merge.

---

## Summary

| Category          | Status         | Notes                                                |
| ----------------- | -------------- | ---------------------------------------------------- |
| Architecture      | ✅ PASS        | Correct base class, SDK V2 used for DataFlow/DataJob |
| Code Organization | ✅ PASS        | Clean file structure, proper separation of concerns  |
| Error Handling    | 🔴 NEEDS FIXES | 2 blockers, 2 warnings                               |
| Type Safety       | 🟡 NEEDS FIXES | 1 dead code block, 2 design issues                   |
| Test Quality      | 🔴 NEEDS FIXES | 1 anti-pattern blocker, 3 missing critical paths     |
| Code Complexity   | 🔴 NEEDS FIXES | 2 blockers (dead code, DRY violation)                |
| Performance       | ✅ PASS        | Generators used throughout, no N+1 patterns          |
| Security          | ✅ PASS        | No hardcoded secrets, no SQL injection risk          |
| Documentation     | ✅ PASS        | All config fields documented with descriptions       |

---

## 🔴 BLOCKERS (Must Fix)

### B1 — Silent exception swallow in `state.json` parsing

**`dlt_client.py:269`** — `except Exception: pass` with no logging. When `state.json` is corrupt or unreadable, `destination` and `dataset_name` silently become `""`, breaking outlet URN construction and lineage for that pipeline. The user sees no indication in the ingestion report.

**Fix:** Add `logger.warning("Failed to parse state.json for pipeline '%s': %s", pipeline_name, e)` in the except block.

---

### B2 — `get_run_history` query failure not surfaced to report

**`dlt.py:372-380`** — When dlt IS installed but the `_dlt_loads` query fails (wrong credentials, missing table, network error), `get_run_history` returns `[]` and logs at module level only. The current guard `if not self.client._dlt_available:` only catches the "dlt not installed" case. The "query failed" case produces zero workunits and zero report entries — silent failure.

**Fix:** Add an `else` branch that also calls `self.report.warning()` and `self.report.report_run_history_error()` when dlt is available but loads are empty.

---

### B3 — `test_run_history_status_mapping` tests a local copy, not production code

**`tests/unit/dlt/test_dlt_source.py:267-277`** — This test creates its own `status_map = {0: InstanceRunResult.SUCCESS}` dict and asserts on that local variable. It never invokes any production code. If `dlt.py:383` was changed to `{0: InstanceRunResult.FAILURE}`, this test would still pass.

**Fix:** Replace with a test that mocks `self.client.get_run_history` to return a `DltLoadInfo(status=0, ...)` and verifies a `DataProcessInstance` entity is emitted with the correct run result.

---

### B4 — No test for `include_lineage=False`

**`tests/integration/dlt/test_dlt.py`** — The `include_lineage` flag gates two code paths (`_build_outlet_urns` at `dlt.py:263` and `_build_fine_grained_lineages` at `dlt.py:334`). There is zero test coverage for `include_lineage=False`. This is a primary configuration option with a distinct code path.

**Fix:** Add an integration test with `extra_config={"include_lineage": False}` and assert that all `outputDatasets` arrays are empty.

---

### B5 — DRY violation: column-parsing logic duplicated

**`dlt_client.py:66-78` and `dlt_client.py:208-222`** — `_parse_schema_file` and `_schema_obj_to_info` contain structurally identical loops that build `DltColumnInfo` lists. Both use the same `col_def.get(...)` pattern because the dlt SDK exposes tables as dicts. Any change to column parsing must be made in two places.

**Fix:** Extract a module-level `_parse_columns(columns_dict)` helper and a `_parse_table(table_name, table_def)` helper.

---

## 🟡 WARNINGS (Should Fix)

### W1 — Empty schemas not surfaced to report

**`dlt_client.py:251-252`** — When `_read_schemas_from_filesystem` returns `[]`, only a module-level `logger.warning` is emitted. `schema_read_errors` not incremented, no `report.warning()` called. A pipeline with unparseable schema files produces a DataFlow with zero DataJob children, silently.

**Fix:** In `dlt.py:get_workunits_internal`, after `get_pipeline_info()`, add: if `pipeline_info.schemas` is empty, emit `self.report.warning(title="No schemas found for pipeline", ...)`.

---

### W2 — `_schema_obj_to_info` failures not counted in `schema_read_errors`

**`dlt_client.py:238-240`** — SDK-path schema conversion failure logs but never increments `schema_read_errors`. Root cause: `DltClient` has no reference to the report object.

**Fix (architectural):** Pass the report into `DltClient.__init__` so client methods can call `self.report.warning(...)` directly.

---

### W3 — `DltRunHistoryConfig.enabled` is a redundant second gate

**`config.py:79-82`** — Two independent booleans both control run history: `include_run_history` (outer, default `False`) and `run_history_config.enabled` (inner, default `True`). The inner gate is functionally dead. Behavior bug: when `enabled=False`, `get_run_history` is called with no time filter rather than being skipped.

**Fix:** Remove `DltRunHistoryConfig.enabled`. Let `include_run_history` be the sole gate.

---

### W4 — `table_outlet_urns` is dead code

**`dlt.py:165-166, 194`** — Built in every pipeline loop but never read. Comment says "for run history DPI construction" but `_emit_run_history` doesn't use it.

**Fix:** Remove lines 165-166 and 194.

---

### W5 — No test for column-level lineage (`_build_fine_grained_lineages`)

**`tests/unit/dlt/`** — The CLL code path at `dlt.py:322-355` is entirely untested. All golden file entries show `fineGrainedLineages: []`.

**Fix:** Add a unit test with `source_table_dataset_urns` providing exactly one inlet + one outlet with user columns, asserting `FineGrainedLineageClass` entries are produced.

---

### W6 — No test for missing `destination_platform_map` entry

**`tests/integration/dlt/`** — Fallback path at `dlt.py:269` (2-part URN when destination not in map) is untested.

**Fix:** Add integration test with empty `destination_platform_map: {}` and assert 2-part URN format.

---

## ℹ️ SUGGESTIONS (Optional)

| #   | Location                                             | Description                                                                               |
| --- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| S1  | `dlt_client.py:14,27-28`                             | `if TYPE_CHECKING: pass` is dead code; remove `TYPE_CHECKING` import too                  |
| S2  | `dlt_client.py:199`                                  | `Any` for `schema_obj` lacks justification in docstring                                   |
| S3  | `config.py:209-212`                                  | `get_pipelines_dir` property uses `get_` prefix (Java style) and is unused                |
| S4  | `dlt.py:373`                                         | Cross-class access of private `_dlt_available`; expose as public `dlt_available` property |
| S5  | `dlt.py:383-385`                                     | `status_map` defined per call; move to module-level constant `_DLT_LOAD_STATUS_MAP`       |
| S6  | `dlt.py:361-429`                                     | `_emit_run_history` is 68 lines; split into coordinator + `_build_dpi_workunits` helper   |
| S7  | `dlt_client.py:128-135`                              | `list_pipeline_names` uses nested-if append; replace with sorted generator                |
| S8  | `dlt.py:166,234`                                     | `dict[str,...]` mixed with `Dict[str,...]` from `typing` in same file                     |
| S9  | `tests/integration/dlt/golden/dlt_golden.json:70-71` | Golden file contains `/Users/maggiehayes/...` absolute paths                              |
| S10 | `tests/integration/dlt/test_dlt.py:157-165`          | `extra_config` in `test_dlt_outlet_urns` is identical to defaults                         |

---

## What's Working Well

- **SDK V2 adoption is correct** — `DataFlow` and `DataJob` from `datahub.sdk` used throughout; all required aspects auto-emit correctly
- **Dual-path client design is solid** — SDK-first with filesystem YAML fallback is the right approach
- **Config documentation is thorough** — every field has a meaningful `description=` with examples
- **`report.warning` LiteralString compliance is clean** — all `title=` and `message=` args are constant strings; f-strings only in `context=`
- **System table filtering is correct** — `DLT_SYSTEM_TABLES` frozenset correctly excludes dlt internal tables
- **`@time_machine.travel` used correctly** — integration tests use time-machine (not freezegun)
- **Performance is appropriate** — generators throughout, no N+1 patterns
