"""Unit tests for ValidationPhase Phase 9/10 helpers."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

from tests.zdu.framework.context import (
    IndexState,
    RollingRestartResult,
    RuntimeMigrationProbe,
    RuntimeMigrationResult,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
)
from tests.zdu.framework.phases.validation import ValidationPhase
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _ctx_with_post_upgrade_state(
    *,
    rolling_restart: RollingRestartResult | None = None,
    dual_write_disabled: list[str] | None = None,
    nonblocking_indices: list[IndexState] | None = None,
) -> TestContext:
    ctx = TestContext()
    ctx.upgrade_blocking = UpgradeBlockingResult(
        indices=[
            IndexState(
                alias="dashboardindex_v2",
                old_backing_index_name="dashboardindex_v2_old",
                next_index_name="dashboardindex_v2_new",
                source_doc_count=100,
                status="COMPLETED",
            ),
            IndexState(
                alias="datasetindex_v2",
                old_backing_index_name="datasetindex_v2_old",
                next_index_name="datasetindex_v2_new",
                source_doc_count=200,
                status="COMPLETED",
            ),
        ]
    )
    ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
        indices=nonblocking_indices
        if nonblocking_indices is not None
        else list(ctx.upgrade_blocking.indices),
        dual_write_disabled_indices=dual_write_disabled or [],
    )
    ctx.rolling_restart = rolling_restart
    return ctx


class TestCheckDualWriteState:
    def test_all_invariants_satisfied_returns_no_failures(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            rolling_restart=RollingRestartResult(
                services_restarted=["datahub-gms-debug"],
                dual_write_start_times={
                    "dashboardindex_v2_old": 1700000000,
                    "datasetindex_v2_old": 1700000000,
                },
            ),
            dual_write_disabled=["dashboardindex_v2_old", "datasetindex_v2_old"],
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        # No FAIL records — both aliases have next_index_name, both old indices
        # have dual-write start times, and disabled set matches blocking set.
        assert all(r.status != "FAIL" for r in results)

    def test_alias_missing_next_index_name_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state()
        assert ctx.upgrade_blocking is not None
        ctx.upgrade_blocking.indices[0].next_index_name = None
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any(
            "dashboardindex_v2" in r.actual_result
            and "next_index_name" in r.actual_result
            for r in fails
        )

    def test_dual_write_disabled_for_unknown_index_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            dual_write_disabled=["foo_v2_old"],  # not in blocking.indices
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any("foo_v2_old" in r.actual_result for r in fails)

    def test_rolling_restart_missing_dual_write_start_time_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            rolling_restart=RollingRestartResult(
                services_restarted=["datahub-gms-debug"],
                dual_write_start_times={"dashboardindex_v2_old": 1700000000},
                # datasetindex_v2_old missing
            ),
            dual_write_disabled=[],
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any("datasetindex_v2_old" in r.actual_result for r in fails)

    def test_no_rolling_restart_skips_dual_write_start_time_check(self) -> None:
        # rolling_restart is None — dual-write start-time check must NOT fail.
        ctx = _ctx_with_post_upgrade_state(rolling_restart=None)
        results = ValidationPhase._check_dual_write_state(ctx)
        assert not any(
            "dual_write_start_times" in r.actual_result.lower()
            for r in results
            if r.status == "FAIL"
        )

    def test_empty_disabled_set_emits_no_synthetic_result(self) -> None:
        # The "empty disabled set on non-empty stack" condition is the same
        # G20c blocker Suite D TC-205 / TC-206 already report. No synthetic
        # SKIP is emitted from here — would double-count the signal.
        ctx = _ctx_with_post_upgrade_state(dual_write_disabled=[])
        results = ValidationPhase._check_dual_write_state(ctx)
        assert all(r.status != "SKIP" for r in results)


# ---------- _check_runtime_migration_probes tests ----------


def _probe(
    mode: str, observed: int, expected: int = 4, error: str | None = None
) -> RuntimeMigrationProbe:
    return RuntimeMigrationProbe(
        urn=f"urn:li:dashboard:(test,zdu-rt-{mode})",
        aspect_name="embed",
        mode=mode,  # type: ignore[arg-type]
        observed_version=observed,
        expected_version=expected,
        timestamp=datetime.utcnow(),
        error=error,
    )


class TestCheckRuntimeMigrationProbes:
    def test_no_runtime_migration_returns_no_failures(self) -> None:
        ctx = TestContext()
        assert ctx.runtime_migration is None
        results = ValidationPhase._check_runtime_migration_probes(ctx)
        assert results == []

    def test_all_passed_probes_returns_no_failures(self) -> None:
        ctx = TestContext()
        ctx.runtime_migration = RuntimeMigrationResult(
            read_probes=[_probe("read", 4), _probe("read", 4)],
            write_probes=[_probe("write", 4)],
        )
        results = ValidationPhase._check_runtime_migration_probes(ctx)
        assert results == []

    def test_failed_read_probes_emit_one_fail(self) -> None:
        ctx = TestContext()
        ctx.runtime_migration = RuntimeMigrationResult(
            read_probes=[_probe("read", 2), _probe("read", 4)],
            write_probes=[_probe("write", 4)],
        )
        results = ValidationPhase._check_runtime_migration_probes(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert "read-path" in fails[0].name
        assert "1/2 read probes failed" in fails[0].actual_result

    def test_failed_write_probes_emit_one_fail(self) -> None:
        ctx = TestContext()
        ctx.runtime_migration = RuntimeMigrationResult(
            read_probes=[_probe("read", 4)],
            write_probes=[_probe("write", 0, error="GMS unavailable")],
        )
        results = ValidationPhase._check_runtime_migration_probes(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert "write-path" in fails[0].name
        assert "GMS unavailable" in fails[0].actual_result

    def test_both_modes_failing_emits_two_separate_fails(self) -> None:
        ctx = TestContext()
        ctx.runtime_migration = RuntimeMigrationResult(
            read_probes=[_probe("read", 2)],
            write_probes=[_probe("write", 1)],
        )
        results = ValidationPhase._check_runtime_migration_probes(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 2
        names = {r.name for r in fails}
        assert "RuntimeMigration[read-path]" in names
        assert "RuntimeMigration[write-path]" in names


# ---------- _check_es_field_presence tests ----------


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
        scenarios = [_es_scenario(tc=315, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        assert results == []
        es.get_doc.assert_called_once()
        # Verifies alias derivation: dashboard → dashboardindex_v2
        idx_arg = es.get_doc.call_args.args[0]
        assert idx_arg == "dashboardindex_v2"

    def test_missing_field_emits_fail(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"renderUrl": "http://x"}  # no embedType
        scenarios = [_es_scenario(tc=315, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert fails[0].tc_number == 315
        assert "embedType" in fails[0].actual_result
        assert "missing" in fails[0].actual_result.lower()

    def test_doc_not_found_emits_fail(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = None  # 404 from ES
        scenarios = [_es_scenario(tc=315, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert fails[0].tc_number == 315
        assert "not found" in fails[0].actual_result.lower()

    def test_dataset_entity_type_uses_dataset_alias(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"someField": True}
        scenarios = [
            _es_scenario(
                tc=320, entity_type="dataset", expected_es_fields=["someField"]
            )
        ]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        assert results == []
        idx_arg = es.get_doc.call_args.args[0]
        assert idx_arg == "datasetindex_v2"

    def test_unknown_entity_type_emits_fail(self) -> None:
        es = MagicMock()
        scenarios = [
            _es_scenario(tc=99, entity_type="chart", expected_es_fields=["someField"])
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
        scenarios = [_es_scenario(tc=315, expected_es_fields=["embedType"])]
        results = ValidationPhase._check_es_field_presence(scenarios, es=es)
        fails = [r for r in results if r.status == "FAIL"]
        assert len(fails) == 1
        assert "ES unreachable" in fails[0].actual_result
