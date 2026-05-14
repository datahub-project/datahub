# ZDU E2E — Plan 9: Validation Dimension 4 (ES Field Presence)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the last unimplemented validation dimension from design doc §5.10. Adds an optional `expected_es_fields: list[str] | None` field to `ZDUTestScenario` and a `_check_es_field_presence(ctx, scenarios, es)` static method on `ValidationPhase`. After Phase 8 (`UpgradeNonBlockingPhase`) completes the sweep + catch-up, Phase 10 verifies that for every scenario with `expected_es_fields` set, the entity's ES document at the entity-type alias contains every named field in its `_source`. Failures are emitted as `ValidationResult` records (FAIL); the phase still does not fail-fast.

**Architecture:** Two changes. (1) `ZDUTestScenario` gets one new optional field with a default of `None` — backward compatible. The `_aspect_migration` factory helper in `scenarios.py` accepts the new kwarg. TC-015 ("ES reindexing after migration", `action="es"`) is the only scenario in Suite A that opts in for now; `expected_es_fields=["embedType"]` because v4 of the embed aspect adds that field. (2) `ValidationPhase._check_es_field_presence` walks the scenarios list, derives the ES alias from the scenario's `entity_type` (dashboard → `dashboardindex_v2`, dataset → `datasetindex_v2`), URL-encodes the URN as the doc-id, calls `es.get_doc(alias, doc_id)`, and emits one FAIL per missing field. Skipped entirely when `ctx` lacks an `ElasticsearchClient` or no scenarios opt in.

**Tech Stack:** Python 3 (existing). Reuses `ElasticsearchClient.get_doc` (already URL-encodes via the F-1 fix). No new dependencies.

**Out of scope (deferred):**

- ES index-mapping schema checks (verifying the `_mapping` has the new field type) — separate plan once schema-stability matters.
- Nested field-path checks (e.g., `embed.embedType.someInner`) — use top-level field names only. Top-level coverage is sufficient for Suite A's two aspects (`embed`, `globalTags`).
- Per-scenario alias overrides (someone wants to query a non-default alias) — defer until a TC actually needs it.
- ES query DSL beyond doc-id lookup — not needed for Dim-4 presence checks.
- Multi-field-value-equality (asserting field VALUES match, not just presence) — Phase 1 (Dim-1) already covers value correctness via the per-scenario validator; Dim-4 is presence-only.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── scenario_loader.py                   MODIFY — add expected_es_fields field on ZDUTestScenario
├── scenarios.py                         MODIFY — _aspect_migration accepts kwarg; populate TC-015
├── phases/
│   └── validation.py                    MODIFY — add _check_es_field_presence + invoke from run()
├── test_validation.py                   MODIFY — add 5 dim-4 tests
└── README.md                            MODIFY — document Dimension 4
```

---

## Task 1: Add `expected_es_fields` to `ZDUTestScenario` + populate TC-015

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenario_loader.py`
- Modify: `smoke-test/tests/zdu/framework/scenarios.py`

**Pattern:** New optional dataclass field with default `None`. Factory helper accepts the kwarg, threads it through. Backward compatible — existing 22 scenarios unaffected.

- [ ] **Step 1: Add the field on `ZDUTestScenario`**

In `scenario_loader.py`, locate the `ZDUTestScenario` dataclass. Add AFTER the existing `suite: Suite = Suite.A` line:

```python
    # Dimension 4 (ES field presence) — Phase 10 validates that the entity's
    # ES document at the entity-type alias contains every named field in its
    # ``_source``. ``None`` (the default) opts the scenario out of Dim-4
    # validation; an empty list opts in but asserts no specific fields.
    expected_es_fields: list[str] | None = None
```

`field` import is already present.

- [ ] **Step 2: Thread the kwarg through the factory**

In `scenarios.py`, locate the `_aspect_migration(...)` helper. Add a new parameter (keep the keyword-arg shape — it's the public surface for Suite-A scenario authors):

```python
def _aspect_migration(
    *,
    tc: int,
    name: str,
    aspect_name: str,
    entity_type: str,
    action: str,
    expected_schema_version: int | None,
    starting_schema_version: int | None = None,
    description: str = "",
    expected_result: str = "",
    details: str = "",
    category: str = "",
    expected_es_fields: list[str] | None = None,
) -> ZDUTestScenario:
```

Inside the function body, pass it through to `ZDUTestScenario(...)`:

```python
        scenario_type="aspect_migration",
        suite=Suite.A,
        expected_es_fields=expected_es_fields,
    )
```

- [ ] **Step 3: Opt TC-015 in**

In `scenarios.py`, locate the `tc=15` entry ("ES reindexing after migration"). Add the new kwarg:

```python
    _aspect_migration(
        tc=15,
        name="ES reindexing after migration",
        aspect_name="embed",
        entity_type="dashboard",
        action="es",
        expected_schema_version=4,
        category="Multi Hop Migration",
        expected_es_fields=["embedType"],
    ),
```

`embedType` is the v4-introduced field that v1 entities don't have until the chain runs and the MAE indexes the migrated aspect.

- [ ] **Step 4: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.scenarios import load_scenarios
scenarios = load_scenarios()
es_opted_in = [s for s in scenarios if s.expected_es_fields is not None]
print(f'opted-in: {len(es_opted_in)}')
for s in es_opted_in:
    print(f'  TC-{s.tc_number:03d}: {s.expected_es_fields}')
assert len(es_opted_in) == 1
assert es_opted_in[0].tc_number == 15
assert es_opted_in[0].expected_es_fields == ['embedType']
print('OK')
"
```

Expected: `opted-in: 1`, `TC-015: ['embedType']`, then `OK`.

- [ ] **Step 5: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 189 pass (existing baseline; the new field doesn't break any existing tests).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenario_loader.py \
        smoke-test/tests/zdu/framework/scenarios.py
git commit -m "feat(zdu): add expected_es_fields on ZDUTestScenario; opt TC-015 in for Dim-4"
```

Re-stage if pre-commit reformats.

---

## Task 2: Implement `_check_es_field_presence` + invocation + tests

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/validation.py`
- Modify: `smoke-test/tests/zdu/framework/test_validation.py`

**Pattern:** Static method symmetric to `_check_dual_write_state` and `_check_runtime_migration_probes`. Walks the scenarios list, queries ES once per opted-in scenario, emits one FAIL per missing field. The `ElasticsearchClient` is already on `self._es` in `ValidationPhase.__init__` (already optional `| None`).

The entity-type → alias mapping is straightforward in Suite A:

| `entity_type` | ES alias            |
| ------------- | ------------------- |
| `dashboard`   | `dashboardindex_v2` |
| `dataset`     | `datasetindex_v2`   |

Other entity types are not yet exercised by Suite A; if more are needed later, extend the mapping.

### 2.1 — Write failing tests

- [ ] **Step 1: Append tests to `test_validation.py`**

At the END of `test_validation.py`, append:

```python
# ---------- _check_es_field_presence tests ----------


from unittest.mock import MagicMock

from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _es_scenario(
    tc: int = 15,
    entity_type: str = "dashboard",
    expected_es_fields: list[str] | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="",
        name=f"ES TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=4,
        action="es",
        aspect_name="embed",
        entity_type=entity_type,
        expected_to_fail=False,
        skip_reason=None,
        scenario_type="aspect_migration",
        suite=Suite.A,
        expected_es_fields=expected_es_fields,
    )


class TestCheckEsFieldPresence:
    def test_no_es_client_returns_no_failures(self) -> None:
        scenarios = [_es_scenario(expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=None)
        assert results == []

    def test_no_opted_in_scenarios_returns_no_failures(self) -> None:
        # All scenarios with expected_es_fields=None.
        scenarios = [_es_scenario(expected_es_fields=None)]
        es = MagicMock()
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        assert results == []
        es.get_doc.assert_not_called()

    def test_all_fields_present_returns_no_failures(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"embedType": "EXTERNAL", "renderUrl": "http://x"}
        scenarios = [_es_scenario(tc=15, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        assert results == []
        es.get_doc.assert_called_once()
        # Verifies alias derivation: dashboard → dashboardindex_v2
        idx_arg = es.get_doc.call_args.args[0]
        assert idx_arg == "dashboardindex_v2"

    def test_missing_field_emits_fail(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"renderUrl": "http://x"}  # no embedType
        scenarios = [_es_scenario(tc=15, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert fails[0].tc_number == 15
        assert "embedType" in fails[0].actual_result
        assert "missing" in fails[0].actual_result.lower()

    def test_doc_not_found_emits_fail(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = None  # 404 from ES
        scenarios = [_es_scenario(tc=15, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert fails[0].tc_number == 15
        assert "not found" in fails[0].actual_result.lower()

    def test_dataset_entity_type_uses_dataset_alias(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"someField": True}
        scenarios = [
            _es_scenario(tc=20, entity_type="dataset", expected_es_fields=["someField"])
        ]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        assert results == []
        idx_arg = es.get_doc.call_args.args[0]
        assert idx_arg == "datasetindex_v2"

    def test_unknown_entity_type_emits_fail(self) -> None:
        es = MagicMock()
        scenarios = [
            _es_scenario(
                tc=99, entity_type="chart", expected_es_fields=["someField"]
            )
        ]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert "chart" in fails[0].actual_result.lower()
        # ES not called when alias can't be derived.
        es.get_doc.assert_not_called()

    def test_es_client_exception_emits_fail(self) -> None:
        es = MagicMock()
        es.get_doc.side_effect = RuntimeError("ES unreachable")
        scenarios = [_es_scenario(tc=15, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert "ES unreachable" in fails[0].actual_result
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_validation.py -v 2>&1 | tail -15
```

Expected: `AttributeError: type object 'ValidationPhase' has no attribute '_check_es_field_presence'` for all 8 new tests.

### 2.2 — Implement the helper

- [ ] **Step 3: Add the static method**

In `phases/validation.py`, add the following AFTER `_check_runtime_migration_probes` (and before `_check_dual_write_state`, or after — order doesn't matter; pick one and stick with it). Use the `Read` tool to find the exact insertion point.

```python
    _ENTITY_TYPE_TO_ES_ALIAS: dict[str, str] = {
        "dashboard": "dashboardindex_v2",
        "dataset": "datasetindex_v2",
    }

    @staticmethod
    def _check_es_field_presence(
        scenarios: list[ZDUTestScenario],
        es: ElasticsearchClient | None,
    ) -> list[ValidationResult]:
        """Phase 10 Dimension 4 — Elasticsearch field presence.

        For each scenario with ``expected_es_fields`` set, query the entity-type
        alias for the URN and verify each named field appears in the ES
        ``_source``. Emits one ``ValidationResult`` (FAIL) per missing field,
        per missing doc, or per unknown entity-type. Returns an empty list if
        no ES client is available or no scenario opted in.
        """
        if es is None:
            return []
        opted_in = [s for s in scenarios if s.expected_es_fields is not None]
        if not opted_in:
            return []

        results: list[ValidationResult] = []
        for scenario in opted_in:
            urn = _make_urn(scenario)
            alias = ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS.get(scenario.entity_type)
            if alias is None:
                results.append(
                    ValidationResult(
                        tc_number=scenario.tc_number,
                        name=f"ES[TC-{scenario.tc_number}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"unknown entity_type '{scenario.entity_type}' — "
                            f"add to ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS"
                        ),
                        failure_reason="entity-type→alias mapping missing",
                    )
                )
                continue
            try:
                doc = es.get_doc(alias, urn)
            except Exception as exc:
                results.append(
                    ValidationResult(
                        tc_number=scenario.tc_number,
                        name=f"ES[TC-{scenario.tc_number}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"ES query failed for alias='{alias}' urn='{urn}': {exc}"
                        ),
                        failure_reason="ES query exception",
                    )
                )
                continue
            if doc is None:
                results.append(
                    ValidationResult(
                        tc_number=scenario.tc_number,
                        name=f"ES[TC-{scenario.tc_number}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"document not found at alias='{alias}' for urn='{urn}'"
                        ),
                        failure_reason="ES document missing",
                    )
                )
                continue
            expected_fields = scenario.expected_es_fields or []
            missing = [f for f in expected_fields if f not in doc]
            if missing:
                results.append(
                    ValidationResult(
                        tc_number=scenario.tc_number,
                        name=f"ES[TC-{scenario.tc_number}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"alias='{alias}' urn='{urn}': fields missing from "
                            f"_source: {missing}"
                        ),
                        failure_reason="ES field(s) missing",
                    )
                )
        return results
```

You'll also need to add the imports at the top of `validation.py`:

```python
from ..es_client import ElasticsearchClient  # likely already imported
from ..scenario_loader import ZDUTestScenario, _make_urn
```

`_make_urn` is at module level in `scenario_loader.py` — it's lowercase-prefixed but considered exported within the framework package (it's already imported by other phases). If it's not currently importable, expose it (rename or add a re-export).

- [ ] **Step 4: Invoke the helper from `run()`**

In `ValidationPhase.run()`, locate the existing dimension-block invocations (read progression → runtime migration → dual-write state). Insert AFTER the runtime-migration block, BEFORE the dual-write block:

```python
        # Dimension 4 — Elasticsearch field presence
        for r in self._check_es_field_presence(self._scenarios, self._es):
            results.append(r)
            if r.status == "FAIL":
                log.warning("ES[TC-%03d] FAIL: %s", r.tc_number, r.actual_result)
```

Note: the field is `self._scenarios` (singular `scenario_results` was an old field name). Verify by reading `validation.py`.

- [ ] **Step 5: Run validator tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_validation.py -v 2>&1 | tail -15
```

Expected: 8 new tests pass (existing 11 also pass — total 19 in `test_validation.py`).

- [ ] **Step 6: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 197 pass (189 baseline + 8 new).

- [ ] **Step 7: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/validation.py \
        smoke-test/tests/zdu/framework/test_validation.py
git commit -m "feat(zdu): ValidationPhase Dimension 4 — ES field presence"
```

Re-stage if pre-commit reformats.

---

## Task 3: README — Dimension 4 + scenario authoring

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Add Dimension 4 to the validation section**

Find the existing `### Phase 10: Validation Dimension 5` heading. Insert a new subsection BEFORE it titled `### Phase 10: Validation Dimension 4`:

````markdown
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
    expected_es_fields=["embedType"],   # v4-introduced field
),
```
````

**Behaviour:**

- Scenarios with `expected_es_fields=None` (the default) are skipped — no ES query, no FAIL records.
- Scenarios with `expected_es_fields=[]` (empty list) are queried but assert no specific fields. The alias / doc-id derivation is still verified.
- The entity-type → alias mapping lives at `ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS`. Only `dashboard` and `dataset` are in the map today; add new mappings as new entity types enter Suite A.
- ES query failures (network errors, exceptions) are recorded as FAIL records, not raised — Phase 10 does not fail-fast.
- Document-not-found at the alias is also FAIL — the sweep + MAE indexing is expected to have populated the doc by Phase 10.

````

(The triple-backtick markdown code fence around the python sample needs to be properly escaped if you're inserting via Edit — use `Read` first to see the exact existing format and match it.)

- [ ] **Step 2: Update the existing dimensions list**

If there's a numbered list of dimensions in the README that says "4 / 5 dimensions implemented" — update to "5 / 5".

- [ ] **Step 3: Verify section ordering**

```bash
grep -nE "^##|^###" smoke-test/tests/zdu/README.md | head -20
````

`### Phase 10: Validation Dimension 4` should appear BEFORE `### Phase 10: Validation Dimension 5`.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Validation Dimension 4 (ES field presence)"
```

Re-stage if pre-commit reformats.

---

## Task 4: Live integration check

**Pre-requisite:** Compose stack up; `:datahub-upgrade:bootJar` fresh (the auto-rebuild runs anyway).

- [ ] **Step 1: Spy verifies the helper call shape**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies ValidationPhase Dimension 4."""
from unittest.mock import MagicMock
from tests.zdu.framework.phases.validation import ValidationPhase
from tests.zdu.framework.scenarios import load_scenarios

es = MagicMock()
es.get_doc.return_value = {"embedType": "EXTERNAL"}
scenarios = load_scenarios()
results = ValidationPhase._check_es_field_presence(scenarios, es=es)
print(f"opted-in scenarios: {sum(1 for s in scenarios if s.expected_es_fields is not None)}")
print(f"FAIL records: {sum(1 for r in results if r.status == 'FAIL')}")
assert es.get_doc.call_count == 1  # only TC-015 opted in
assert es.get_doc.call_args.args[0] == "dashboardindex_v2"
print("LIVE-WIRING OK")
PY
```

Expected: ends with `LIVE-WIRING OK` and shows `opted-in scenarios: 1`, `FAIL records: 0`.

- [ ] **Step 2: Suite A regression check**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -40
```

Expected:

- Same baseline 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP.
- The validation `details.failures` block does NOT contain a Dim-4 FAIL for TC-015 — by Phase 10 the embed v4 mutator will have run on the dashboard `(test,zdu-tc-15)` URN, MAE will have indexed the migrated doc, and `embedType` should be present in the ES `_source`.
- If Dim-4 FAILs for TC-015, inspect by hand: query the dashboard alias directly (`curl http://localhost:9200/dashboardindex_v2/_doc/<urlencoded-urn>`) and confirm whether `embedType` is in the response. If it isn't, this is a real bug in the mutator chain or MAE indexing, not a Plan 9 issue.

- [ ] **Step 3: Inspect the JSON report**

```bash
python3 -c "
import json
data = json.load(open('<REPO_ROOT>/smoke-test/smoke-test/build/zdu-test-report.json'))
val = next((p for p in data['phases'] if p['name'] == 'validation'), None)
if val:
    fails = val.get('details', {}).get('failures', [])
    es_fails = [f for f in fails if f.get('name', '').startswith('ES[TC-')]
    print(f'ES[TC-*] FAIL records: {len(es_fails)}')
    for f in es_fails:
        print(f'  {f}')
"
```

Expected: `ES[TC-*] FAIL records: 0` on a clean run.

- [ ] **Step 4: Cleanup any test entities**

```bash
docker compose -f <REPO_ROOT>/docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-tc-%)' OR urn LIKE 'urn:li:dataset:(urn:li:dataPlatform:test,zdu-tc-%';
" 2>&1 | tail -3
```

(Optional — these are reseeded fresh on every run.)

- [ ] **Step 5: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): Plan 9 live-wiring fix"
```

If nothing regressed, no commit needed.

---

## Task 5: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff 54c9c78f81..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-9.diff
wc -l /tmp/zdu-plan-9.diff
```

(`54c9c78f81` is the bootJar-dep commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-9.diff`. This PR implements Validation Dimension 4 (ES field presence) — the last unimplemented validation dimension from design doc §5.10.
>
> Concretely:
>
> - `ZDUTestScenario.expected_es_fields: list[str] | None = None` — new optional field, default opts out.
> - `_aspect_migration(...)` factory in `scenarios.py` accepts `expected_es_fields` kwarg.
> - TC-015 ("ES reindexing after migration") opts in with `["embedType"]` (the v4-introduced field on the embed aspect).
> - `ValidationPhase._check_es_field_presence(scenarios, es)` — static method that walks opted-in scenarios, derives the entity-type alias, queries the doc, emits FAIL per missing field / missing doc / unknown entity-type / ES exception.
> - `ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS` — class-level dict mapping `dashboard`→`dashboardindex_v2`, `dataset`→`datasetindex_v2`.
> - 8 unit tests covering: no-ES-client, no-opted-in, all-fields-present, missing-field, missing-doc, dataset-alias, unknown-entity-type, ES-exception.
> - Invocation block added to `ValidationPhase.run()` between the runtime-migration block and the dual-write block.
> - README documents Dim-4 + how to opt scenarios in.
>
> Check specifically:
>
> 1. **Backward compat:** the new field has a default of `None`. The 22 existing scenarios in `scenarios.py` are NOT modified to add the kwarg. Confirm by grepping that only TC-015's entry has the new kwarg.
> 2. **`None` vs `[]` semantics:** `None` opts the scenario out entirely (no ES query). `[]` opts in but asserts no specific fields (the alias / doc-id derivation is still verified). Test `test_no_opted_in_scenarios_returns_no_failures` verifies the None path.
> 3. **Entity-type→alias mapping:** living in `ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS` (class attribute). Adding new types is a one-line dict insertion.
> 4. **Failure-mode classification:** unknown entity-type / ES exception / doc-not-found / field-missing all produce FAIL records. The phase still does not fail-fast — Phase 10's "collect all failures" semantic is preserved.
> 5. **`_make_urn` import:** the helper is already used by other phases (likely `inject_traffic_pre`, `inject_traffic_dual`). Confirm the import path works without circular-import problems.
> 6. **No fail-on-empty-list:** `expected_es_fields=[]` should NOT emit a FAIL — the loop `for f in expected_fields` is a no-op when the list is empty. Verify.
> 7. **`get_doc` URL-encoding:** `ElasticsearchClient.get_doc` already URL-encodes the doc-id (Plan F-1 fix). Don't double-encode in the validator.
> 8. **Test quality:** all 8 tests use mocked `ElasticsearchClient`; behaviour-focused; no implementation-detail assertions.
> 9. **YAGNI:** No nested-field-path support, no per-scenario alias overrides, no value-equality checks. All deferrals are honoured per the plan's Out-of-scope list.
> 10. **Type hints complete:** `expected_es_fields: list[str] | None`, `_check_es_field_presence(...) -> list[ValidationResult]`.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 9"
```

---

## Self-Review

**Spec coverage** (against design doc §5.10 Dimension 4):

| Requirement                                      | Task                                                                   |
| ------------------------------------------------ | ---------------------------------------------------------------------- |
| For ES-bearing scenarios, query the search index | Task 2 (`es.get_doc(alias, urn)`)                                      |
| Confirm migrated fields appear in results        | Task 2 (`f not in doc` check)                                          |
| The index mapping reflects the new shape         | OUT OF SCOPE — separate plan                                           |
| Validation collects ALL failures (no fail-fast)  | EXISTING — preserved by emitting via `results.append(...)`             |
| Per-failure: URN, expected, got, reason          | Task 2 (`actual_result` + `failure_reason` on each `ValidationResult`) |

**Placeholder scan:** None.

**Type / signature consistency:**

- `ZDUTestScenario.expected_es_fields: list[str] | None = None` (Task 1) — read by Task 2's helper.
- `ValidationPhase._check_es_field_presence(scenarios: list[ZDUTestScenario], es: ElasticsearchClient | None) -> list[ValidationResult]` (Task 2).
- `ValidationPhase._ENTITY_TYPE_TO_ES_ALIAS: dict[str, str]` — class attribute (Task 2).
- `_make_urn` from `scenario_loader` — already exists; reused.

**Risks called out:**

1. **`embedType` on TC-015.** TC-015 is the `action="es"` scenario on a dashboard `embed` aspect. The seeded data is the v1 shape (`{"renderUrl": "..."}`). The mutator chain v1→v2→v3→v4 should rewrite this to v4 shape, which adds `embedType`. If the live run produces a Dim-4 FAIL for TC-015, that's a real ES-indexing bug — investigate the mutator chain output and MAE indexing pipeline before changing the scenario.
2. **Single-image dev runs.** On the dev stack, the v2→v3 mutator is intentionally commented out (TC-007/TC-011/TC-021 XFAIL because of this). TC-015 expects v1 → v4, so the chain stops at v2 and `embedType` is not added. **This may produce a Dim-4 FAIL on dev runs.** If so: (a) document this as a known dev-stack limitation, similar to TC-020's pre-existing FAIL; OR (b) downgrade `expected_es_fields` to `["renderUrl"]` so the assertion holds against v2 too. Decide during Task 4 live integration based on actual ES doc contents.
3. **Alias name stability.** The hardcoded `dashboardindex_v2` / `datasetindex_v2` matches the current DataHub naming convention. If DataHub renames its indices in a future release, the mapping needs updating. Acceptable risk — same coupling already exists in `InjectTrafficPrePhase` / `InjectTrafficDualPhase` for the dashboard alias check.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-9-validation-dim4-es-field-presence.md`.

Per session policy: defaulting to subagent-driven execution.
