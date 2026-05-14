from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Literal, cast

from .base import Phase, PhaseResult
from ..context import IOObservation, TestContext, ValidationResult
from ..es_client import ElasticsearchClient
from ..scenario_executor import ScenarioTypeRegistry
from ..scenario_loader import ZDUTestScenario, _make_urn

log = logging.getLogger(__name__)


class ValidationPhase(Phase):
    name = "validation"

    def __init__(
        self,
        registry: ScenarioTypeRegistry,
        scenarios: list[ZDUTestScenario],
        es: ElasticsearchClient | None = None,
    ) -> None:
        self._registry = registry
        self._scenarios = scenarios
        self._es = es

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        results: list[ValidationResult] = []

        for scenario in self._scenarios:
            result = self._registry.validate(scenario, ctx)
            results.append(result)
            log.info(
                "TC-%03d [%s]: %s", scenario.tc_number, scenario.name, result.status
            )
            if result.status == "FAIL":
                log.warning("  FAIL reason: %s", result.failure_reason)

        # Write path check — all writer results must pass
        write_failures = [r for r in ctx.io_write_results if not r.passed]
        if write_failures:
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="ConcurrentIO write path",
                    status="FAIL",
                    expected_to_fail=False,
                    actual_result=(
                        f"{len(write_failures)} write(s) received wrong schemaVersion "
                        f"during sweep"
                    ),
                    failure_reason=str(write_failures[0]),
                )
            )

        # Read progression check — no URN may move backward in schema version
        regression = self._check_read_progression(ctx.io_observations)
        if regression:
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="Read version progression",
                    status="FAIL",
                    expected_to_fail=False,
                    actual_result=f"Version regression detected: {regression}",
                    failure_reason=regression,
                )
            )

        # Phase 9 runtime probes — read/write mutator chain assertions
        for r in self._check_runtime_migration_probes(ctx):
            results.append(r)
            if r.status == "FAIL":
                log.warning("RuntimeMigration FAIL: %s", r.failure_reason)

        # Dimension 4 — Elasticsearch field presence
        for r in self._check_es_field_presence(self._scenarios, self._es):
            results.append(r)
            if r.status == "FAIL":
                log.warning("ES[TC-%03d] FAIL: %s", r.tc_number, r.actual_result)

        # Dimension 5 — alias targets and dual-write state
        for r in self._check_dual_write_state(ctx):
            results.append(r)
            if r.status == "FAIL":
                log.warning("DualWriteState FAIL: %s", r.failure_reason)

        ctx.validation_results = results

        passed = sum(1 for r in results if r.status == "PASS")
        failed = sum(1 for r in results if r.status == "FAIL")
        xfail = sum(1 for r in results if r.status == "XFAIL")
        skipped = sum(1 for r in results if r.status in ("XPASS", "SKIP"))

        overall: Literal["passed", "failed"] = "failed" if failed > 0 else "passed"
        return PhaseResult(
            phase_name=self.name,
            status=cast(Literal["passed", "failed", "skipped"], overall),
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "total": len(results),
                "passed": passed,
                "failed": failed,
                "xfail": xfail,
                "skipped": skipped,
                "failures": [
                    {
                        "tc": r.tc_number,
                        "name": r.name,
                        "reason": r.failure_reason,
                    }
                    for r in results
                    if r.status == "FAIL"
                ],
            },
        )

    @staticmethod
    def _check_runtime_migration_probes(ctx: TestContext) -> list[ValidationResult]:
        """Consume ``ctx.runtime_migration`` probes captured by Phase 9.

        Emits one ``ValidationResult`` for the read-path mutator chain and one
        for the write-path mutator chain whenever any probe of that mode failed
        (version mismatch or GMS error). Returns an empty list when Phase 9 was
        skipped (``ctx.runtime_migration`` is None).
        """
        rm = ctx.runtime_migration
        if rm is None:
            return []
        results: list[ValidationResult] = []
        failed_reads = [p for p in rm.read_probes if not p.passed]
        if failed_reads:
            first = failed_reads[0]
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="RuntimeMigration[read-path]",
                    status="FAIL",
                    expected_to_fail=False,
                    actual_result=(
                        f"{len(failed_reads)}/{len(rm.read_probes)} read probes failed; "
                        f"first failure: urn={first.urn} "
                        f"observed={first.observed_version} "
                        f"expected={first.expected_version} "
                        f"error={first.error}"
                    ),
                    failure_reason=(
                        "read-path mutator did not produce expected schemaVersion"
                    ),
                )
            )
        failed_writes = [p for p in rm.write_probes if not p.passed]
        if failed_writes:
            first = failed_writes[0]
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="RuntimeMigration[write-path]",
                    status="FAIL",
                    expected_to_fail=False,
                    actual_result=(
                        f"{len(failed_writes)}/{len(rm.write_probes)} write probes failed; "
                        f"first failure: urn={first.urn} "
                        f"observed={first.observed_version} "
                        f"expected={first.expected_version} "
                        f"error={first.error}"
                    ),
                    failure_reason=(
                        "write-path mutator did not produce expected schemaVersion"
                    ),
                )
            )
        return results

    _ENTITY_TYPE_TO_ES_ALIAS: dict[str, str] = {
        "dashboard": "dashboardindex_v2",
        "dataset": "datasetindex_v2",
    }

    @classmethod
    def _check_es_field_presence(
        cls,
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
            alias = cls._ENTITY_TYPE_TO_ES_ALIAS.get(scenario.entity_type)
            if alias is None:
                results.append(
                    ValidationResult(
                        tc_number=scenario.tc_number,
                        name=f"ES[TC-{scenario.tc_number}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"unknown entity_type '{scenario.entity_type}' — "
                            f"add to {cls.__name__}._ENTITY_TYPE_TO_ES_ALIAS"
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

    @staticmethod
    def _check_dual_write_state(ctx: TestContext) -> list[ValidationResult]:
        """Phase 10 Dimension 5 — alias targets and dual-write state.

        Walks ``ctx.upgrade_blocking.indices``, ``ctx.upgrade_nonblocking``, and
        ``ctx.rolling_restart`` and emits one ``ValidationResult`` per invariant
        violation. Returns an empty list when nothing was captured (e.g.,
        upgrade_blocking phase was skipped) — the dimension is then trivially
        satisfied.
        """
        results: list[ValidationResult] = []
        blocking = ctx.upgrade_blocking
        if blocking is None or not blocking.indices:
            return results

        nonblocking = ctx.upgrade_nonblocking
        # Invariant 1: every alias has a non-empty next_index_name.
        for idx in blocking.indices:
            if not idx.next_index_name:
                results.append(
                    ValidationResult(
                        tc_number=0,
                        name=f"DualWriteState[{idx.alias}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"alias '{idx.alias}': missing next_index_name "
                            f"after blocking sweep"
                        ),
                        failure_reason="alias swap target absent",
                    )
                )

        # Invariant 2: every disabled index name was tracked in blocking.indices.
        if nonblocking is not None:
            tracked_old = {
                i.old_backing_index_name
                for i in blocking.indices
                if i.old_backing_index_name
            }
            for name in nonblocking.dual_write_disabled_indices:
                if name not in tracked_old:
                    results.append(
                        ValidationResult(
                            tc_number=0,
                            name=f"DualWriteState[{name}]",
                            status="FAIL",
                            expected_to_fail=False,
                            actual_result=(
                                f"index '{name}' was marked DUAL_WRITE_DISABLED "
                                f"but not present in blocking.indices.old_backing_index_name set"
                            ),
                            failure_reason="disabled-index sanity",
                        )
                    )

        # Invariant 3: rolling restart recorded a dualWriteStartTime per old index.
        if ctx.rolling_restart is not None:
            recorded = ctx.rolling_restart.dual_write_start_times
            for idx in blocking.indices:
                old = idx.old_backing_index_name
                if old and old not in recorded:
                    results.append(
                        ValidationResult(
                            tc_number=0,
                            name=f"DualWriteState[{old}]",
                            status="FAIL",
                            expected_to_fail=False,
                            actual_result=(
                                f"index '{old}' missing from "
                                f"rolling_restart.dual_write_start_times"
                            ),
                            failure_reason="rolling-restart did not record dualWriteStartTime",
                        )
                    )

        # Invariant 4: blocking and nonblocking should describe the same alias set.
        if nonblocking is not None and nonblocking.indices:
            blocking_aliases = {i.alias for i in blocking.indices}
            nb_aliases = {i.alias for i in nonblocking.indices}
            missing_in_nb = blocking_aliases - nb_aliases
            extra_in_nb = nb_aliases - blocking_aliases
            if missing_in_nb or extra_in_nb:
                results.append(
                    ValidationResult(
                        tc_number=0,
                        name="DualWriteState[alias-set]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"alias mismatch — blocking only: {sorted(missing_in_nb)} ; "
                            f"nonblocking only: {sorted(extra_in_nb)}"
                        ),
                        failure_reason="alias set divergence between phases",
                    )
                )

        # Invariant 5 (informational SKIP): empty disabled set on a non-empty stack.
        if (
            nonblocking is not None
            and not nonblocking.dual_write_disabled_indices
            and blocking.indices
        ):
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="DualWriteState[disabled-empty]",
                    status="SKIP",
                    expected_to_fail=False,
                    actual_result=(
                        "no DUAL_WRITE_DISABLED markings observed — "
                        "expected on a dev stack with no migration mutators registered"
                    ),
                )
            )
        return results

    @staticmethod
    def _check_read_progression(observations: list[IOObservation]) -> str | None:
        by_urn: dict[str, list[IOObservation]] = {}
        for obs in observations:
            by_urn.setdefault(obs.urn, []).append(obs)

        for urn, obs_list in by_urn.items():
            sorted_obs = sorted(obs_list, key=lambda o: o.timestamp)
            prev = 0
            for obs in sorted_obs:
                if obs.observed_version < prev:
                    return (
                        f"{urn} moved backward: {prev} → {obs.observed_version} "
                        f"at {obs.timestamp}"
                    )
                prev = obs.observed_version
        return None
