from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .context import ValidationResult
from .datahub_client import DataHubClient
from .suite import Suite

if TYPE_CHECKING:
    from .context import TestContext

log = logging.getLogger(__name__)

KNOWN_FAILURES: dict[int, str] = {
    # bridgeGap is now wired (EmbedV2ToV3Mutator + EmbedV3ToV4Mutator exist in
    # entity-registry/.../aspect/hooks/). TC-307 / TC-311 / TC-321 should now
    # PASS via the full v2→v3→v4 chain at sweep/write time. Removed from XFAIL.
    #
    # TC-312, TC-313, TC-314, TC-319 removed — false positives. TC-312 asserts
    # expected_schema_version=3 (no-op transform leaves schemaVersion at 3,
    # which is correct by design). TC-313 / TC-314 / TC-319 use
    # expected_schema_version=None so the executor only verifies the aspect
    # is fetchable — it does not actually exercise the crash paths the names
    # imply. All four pass under the normal validation path.
}


@dataclass
class ZDUTestScenario:
    tc_number: int
    category: str
    name: str
    description: str
    prerequisite_steps: str
    test_steps: str
    expected_result: str
    current_status: str
    details: str
    starting_schema_version: int | None
    expected_schema_version: int | None
    action: str
    aspect_name: str
    entity_type: str
    expected_to_fail: bool
    skip_reason: str | None
    scenario_type: str = "phase1_reindex"
    suite: Suite = Suite.B
    # Dimension 4 (ES field presence) — Phase 10 validates that the entity's
    # ES document at the entity-type alias contains every named field in its
    # ``_source``. ``None`` (the default) opts the scenario out of Dim-4
    # validation; an empty list opts in but asserts no specific fields.
    expected_es_fields: list[str] | None = None
    # Suite B (phase1_reindex) — set of ES index aliases that MUST appear as
    # real reindexes after SystemUpdateBlocking. The set is the authoritative
    # expectation: TC-101 asserts observed == this set; TC-102 asserts every
    # OTHER captured alias swap was a no-op. ``None`` opts the scenario out
    # of input-driven validation.
    expected_reindex_indices: frozenset[str] | None = None
    # Suite B (phase1_reindex) — minimum number of aliases that must appear
    # in ``ctx.upgrade_blocking.raw`` with ``requiresDataBackfill=true`` AND
    # ``status="COMPLETED"``. Used by TC-105 to assert "multiple indices
    # needed reindex, all completed cleanly". Decoupled from
    # nextIndexName-non-empty (0-doc source still counts here). ``None``
    # opts the scenario out.
    min_real_reindex_count: int | None = None
    # Suite B (phase1_reindex) — set of ES index aliases that MUST appear in
    # ``ctx.upgrade_blocking.indices_updated_in_place`` (captured from the
    # ``Updating index <name> mappings in place`` log line). Used by TC-103
    # to assert the upgrade took the in-place mapping update path for the
    # named indices — i.e., new mapping fields were added but no full
    # reindex was needed (``isPureMappingsAddition=true``). ``None`` opts
    # the scenario out.
    expected_in_place_update_indices: frozenset[str] | None = None


class ScenarioLoader:
    """Returns the codified scenario list. Kept as a class for backward
    compatibility with callers that instantiate it (``ScenarioLoader().load()``).
    """

    def load(self, source: str | None = None) -> list[ZDUTestScenario]:
        # ``source`` is preserved as a kwarg for backward compatibility but
        # ignored — scenarios live in framework/scenarios.py now.
        from .scenarios import load_scenarios

        return load_scenarios()


def _make_urn(scenario: ZDUTestScenario) -> str:
    tc = scenario.tc_number
    if scenario.entity_type == "dataset":
        return f"urn:li:dataset:(urn:li:dataPlatform:test,zdu-tc-{tc},PROD)"
    return f"urn:li:dashboard:(test,zdu-tc-{tc})"


def make_old_data(scenario: ZDUTestScenario) -> dict[str, Any]:
    if scenario.aspect_name == "globalTags":
        return {"tags": [{"tag": "urn:li:tag:zdu-test-tag"}]}
    sv = scenario.starting_schema_version
    if sv is None or sv == 1:
        return {
            "renderUrl": f"http://zdu-test.example.com/embed/tc-{scenario.tc_number}"
        }
    if sv == 2:
        # v2 embed: renderUrl removed, embedType not yet added — valid minimal struct
        return {}
    return {"embedType": "EXTERNAL", "embedTitle": f"ZDU Test TC-{scenario.tc_number}"}


class ScenarioExecutor:
    def __init__(self, datahub: DataHubClient) -> None:
        self._datahub = datahub

    def seed(self, scenario: ZDUTestScenario) -> list[str]:
        """Seed one entity for this scenario. Returns the URN list (empty for XFAIL/rolling).

        Only handles ``scenario_type="aspect_migration"`` — other scenario types
        (e.g., Suite D's ``catch_up``) have their own executors that don't seed
        per-scenario and are validated against pipeline captures by Phase 10.
        """
        if scenario.scenario_type != "aspect_migration":
            return []
        if scenario.expected_to_fail or scenario.action == "rolling":
            return []
        urn = _make_urn(scenario)
        old_data = make_old_data(scenario)
        sys_meta: dict[str, Any] | None = None
        if scenario.starting_schema_version is not None:
            sys_meta = {"schemaVersion": scenario.starting_schema_version}
        self._datahub.ingest_mcp(urn, scenario.aspect_name, old_data, sys_meta)
        return [urn]

    def validate(
        self, scenario: ZDUTestScenario, ctx: "TestContext"
    ) -> ValidationResult:
        # Suite N's sweep-invariant subset (TC-324..031) runs in the same
        # non-blocking phase but doesn't seed per-URN — dispatch to the
        # sweep validators (read sweep-level captures off ctx).
        if scenario.scenario_type == "sweep":
            from .sweep_executor import dispatch_sweep_scenario

            return dispatch_sweep_scenario(scenario, ctx)

        # Build URN list from ctx — replaces the per-call argument the legacy
        # ValidationPhase used to compute on every scenario.
        urns = [e.urn for e in ctx.seeded_entities if e.tc_number == scenario.tc_number]

        if scenario.action == "rolling":
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result="Skipped — rolling upgrade test steps not yet implemented",
            )
        if scenario.expected_to_fail:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="XFAIL",
                expected_to_fail=True,
                actual_result="Expected failure — not reproduced in integration test",
                failure_reason=scenario.skip_reason,
            )
        if not urns:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result="No URNs seeded for this scenario",
            )

        expected_version = scenario.expected_schema_version
        failures: list[str] = []

        for urn in urns:
            try:
                resp = self._datahub.get_aspect(urn, scenario.aspect_name)
                if (
                    expected_version is not None
                    and resp.schema_version != expected_version
                ):
                    failures.append(
                        f"{urn}: expected schemaVersion={expected_version}, "
                        f"got {resp.schema_version}"
                    )
            except Exception as exc:
                failures.append(f"{urn}: {exc}")

        if failures:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="FAIL",
                expected_to_fail=False,
                actual_result="; ".join(failures),
                failure_reason=failures[0],
            )
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"All {len(urns)} URN(s) at expected schemaVersion={expected_version}"
            ),
        )
