"""Unit tests for Phase1ReindexExecutor."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import TestContext, UpgradeBlockingResult
from tests.zdu.framework.phase1_reindex_executor import Phase1ReindexExecutor
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="ES Phase 1 — Incremental Reindex",
        name=f"TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="phase1_reindex",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="phase1_reindex",
        suite=Suite.B,
    )


_ALL_REQUIRED_KEYS = {
    "nextIndexName": "dashboardindex_v2_new",
    "oldBackingIndexName": "dashboardindex_v2_old",
    "reindexStartTime": 1000000,
    "sourceDocCount": 42,
    "taskId": "abc123",
    "requiresDataBackfill": False,
    "status": "COMPLETED",
}


@pytest.fixture
def executor() -> Phase1ReindexExecutor:
    return Phase1ReindexExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty(self, executor: Phase1ReindexExecutor) -> None:
        assert executor.seed(_scenario(tc=108)) == []


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(
            tc=101, expected_to_fail=True, skip_reason="needs mapping diff"
        )
        result = executor.validate(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs mapping diff"


class TestTC108StateShape:
    def test_tc108_passes_when_all_required_keys_present(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={"indicesState": {"dashboardindex_v2": dict(_ALL_REQUIRED_KEYS)}}
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "PASS"

    def test_tc108_fails_when_required_keys_missing(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Remove one required key to trigger FAIL.
        partial = {k: v for k, v in _ALL_REQUIRED_KEYS.items() if k != "taskId"}
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={"indicesState": {"dashboardindex_v2": partial}}
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "FAIL"
        assert "taskId" in (result.actual_result or "")


class TestUnknownTC:
    def test_unknown_tc_returns_skip(self, executor: Phase1ReindexExecutor) -> None:
        scen = _scenario(tc=199)  # not in the registered range
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"


class TestTC101SingleIndexReindex:
    def test_passes_when_real_reindex_observed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # At least one alias swap with non-empty next_index_name → PASS.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("dashboardindex_v2", "dashboardindex_v2_v1_5_0rc1_1778686179335"),
                ("schemafieldindex_v2", ""),  # 0-doc no-op
            ]
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "PASS"
        assert "dashboardindex_v2" in (result.actual_result or "")

    def test_fails_when_all_swaps_were_noop(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Every alias swap had empty next_index_name → no real reindex → FAIL.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ]
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "FAIL"

    def test_skips_when_phase4_did_not_run(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # ctx.upgrade_blocking is None (Phase 4 skipped) → SKIP not FAIL,
        # so a partial run doesn't surface as a fake regression.
        result = executor.validate(_scenario(tc=101), TestContext())
        assert result.status == "SKIP"


class TestTC106MixedReindex:
    def test_passes_when_both_real_and_noop_observed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ]
        )
        result = executor.validate(_scenario(tc=106), ctx)
        assert result.status == "PASS"

    def test_fails_when_only_real_reindexes(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Need both categories — pure real-reindex run isn't "mixed".
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("datasetindex_v2", "datasetindex_v2_new"),
            ]
        )
        result = executor.validate(_scenario(tc=106), ctx)
        assert result.status == "FAIL"

    def test_fails_when_only_noops(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ]
        )
        result = executor.validate(_scenario(tc=106), ctx)
        assert result.status == "FAIL"
