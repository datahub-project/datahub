"""Unit tests for Phase1ReindexExecutor."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import (
    TestContext,
    UpgradeBlockingReRunResult,
    UpgradeBlockingResult,
)
from tests.zdu.framework.phase1_reindex_executor import Phase1ReindexExecutor
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
    expected_reindex_indices: frozenset[str] | None = None,
    min_real_reindex_count: int | None = None,
    expected_in_place_update_indices: frozenset[str] | None = None,
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
        expected_reindex_indices=expected_reindex_indices,
        min_real_reindex_count=min_real_reindex_count,
        expected_in_place_update_indices=expected_in_place_update_indices,
    )


# Per-alias entry shape that satisfies _REQUIRED_INDICES_STATE_KEYS. taskId is
# optional (only present on real reindex), but included here to cover both paths.
_ALL_REQUIRED_KEYS = {
    "status": "COMPLETED",
    "nextIndexName": "dashboardindex_v2_new",
    "sourceDocCount": 42,
    "requiresDataBackfill": True,
    "reindexStartTime": 1000000,
    "reindexCompleteTime": 1000005,
    "taskId": "abc123",
}


@pytest.fixture
def executor() -> Phase1ReindexExecutor:
    return Phase1ReindexExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty(self, executor: Phase1ReindexExecutor) -> None:
        assert executor.seed(_scenario(tc=107)) == []


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


class TestUnknownTC:
    def test_unknown_tc_returns_skip(self, executor: Phase1ReindexExecutor) -> None:
        scen = _scenario(tc=199)  # not in the registered range
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"


class TestTC101InputDrivenReindex:
    def _ctx(self, swaps: list[tuple[str, str]]) -> TestContext:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(alias_swaps_observed=swaps)
        return ctx

    def test_passes_when_observed_matches_expected(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(
            tc=101,
            expected_reindex_indices=frozenset({"dashboardindex_v2"}),
        )
        ctx = self._ctx(
            [
                ("dashboardindex_v2", "dashboardindex_v2_v1_5_new"),
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ]
        )
        result = executor.validate(scen, ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_expected_reindex_is_missing(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(
            tc=101,
            expected_reindex_indices=frozenset({"dashboardindex_v2"}),
        )
        # The expected index never appears with a non-empty new_index_name.
        ctx = self._ctx([("dashboardindex_v2", "")])
        result = executor.validate(scen, ctx)
        assert result.status == "FAIL"
        assert "missing" in (result.actual_result or "")

    def test_fails_when_unexpected_reindex_observed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(
            tc=101,
            expected_reindex_indices=frozenset({"dashboardindex_v2"}),
        )
        ctx = self._ctx(
            [
                ("dashboardindex_v2", "dashboardindex_v2_v1_5_new"),
                ("chartindex_v2", "chartindex_v2_v1_5_new"),  # not expected!
            ]
        )
        result = executor.validate(scen, ctx)
        assert result.status == "FAIL"
        assert "unexpected" in (result.actual_result or "")
        assert "chartindex_v2" in (result.actual_result or "")

    def test_skips_when_expected_set_is_none(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(tc=101)  # no expected_reindex_indices
        ctx = self._ctx([("dashboardindex_v2", "dashboardindex_v2_new")])
        result = executor.validate(scen, ctx)
        assert result.status == "SKIP"

    def test_skips_when_phase4_did_not_run(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(
            tc=101,
            expected_reindex_indices=frozenset({"dashboardindex_v2"}),
        )
        # ctx.upgrade_blocking is None — Phase 4 was skipped.
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"


class TestTC102UnchangedIndices:
    def _ctx_with_tc101(
        self,
        swaps: list[tuple[str, str]],
        tc101_expected: frozenset[str],
    ) -> TestContext:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(alias_swaps_observed=swaps)
        # TC-102 looks up TC-101's expected_reindex_indices via ctx.all_scenarios.
        ctx.all_scenarios = [
            _scenario(tc=101, expected_reindex_indices=tc101_expected),
            _scenario(tc=102),
        ]
        return ctx

    def test_passes_when_only_expected_indices_reindexed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx_with_tc101(
            swaps=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ],
            tc101_expected=frozenset({"dashboardindex_v2"}),
        )
        result = executor.validate(_scenario(tc=102), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_unexpected_index_reindexed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx_with_tc101(
            swaps=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("chartindex_v2", "chartindex_v2_new"),  # violation
            ],
            tc101_expected=frozenset({"dashboardindex_v2"}),
        )
        result = executor.validate(_scenario(tc=102), ctx)
        assert result.status == "FAIL"
        assert "chartindex_v2" in (result.actual_result or "")

    def test_skips_when_tc101_not_in_scenario_list(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[("dashboardindex_v2", "new")]
        )
        # all_scenarios deliberately doesn't include TC-101 — simulates a
        # filtered run via run_only_tc=[102].
        ctx.all_scenarios = [_scenario(tc=102)]
        result = executor.validate(_scenario(tc=102), ctx)
        assert result.status == "SKIP"


class TestTC104EmptySourceNoop:
    def test_passes_when_empty_source_swap_present(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("schemafieldindex_v2", ""),  # empty-source no-op
            ]
        )
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "PASS"
        assert "schemafieldindex_v2" in (result.actual_result or "")

    def test_fails_when_all_swaps_are_real_reindexes(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("datasetindex_v2", "datasetindex_v2_new"),
            ]
        )
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "FAIL"

    def test_cross_check_fails_when_raw_contradicts(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Log says no-op swap, but MySQL says sourceDocCount > 0 — that's a
        # production bug (or test fixture mismatch). Surface it.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[("schemafieldindex_v2", "")],
            raw={"schemafieldindex_v2": {"sourceDocCount": 42, "status": "COMPLETED"}},
        )
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "FAIL"
        assert "sourceDocCount" in (result.actual_result or "")


class TestTC105MultiIndexAllReindex:
    """Real-stack shape: 3 aliases, all requiresDataBackfill=True, all status=COMPLETED."""

    def _real_stack_raw(self) -> dict:
        return {
            "dashboardindex_v2": {
                "requiresDataBackfill": True,
                "status": "COMPLETED",
            },
            "schemafieldindex_v2": {
                "requiresDataBackfill": True,
                "status": "COMPLETED",
            },
            "corpgroupindex_v2": {
                "requiresDataBackfill": True,
                "status": "COMPLETED",
            },
        }

    def test_passes_when_three_backfill_aliases_all_completed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw=self._real_stack_raw())
        result = executor.validate(_scenario(tc=105, min_real_reindex_count=2), ctx)
        assert result.status == "PASS", result.actual_result

    def test_passes_at_boundary(self, executor: Phase1ReindexExecutor) -> None:
        # min == observed → PASS (boundary, ≥ comparison).
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={
                "dashboardindex_v2": {
                    "requiresDataBackfill": True,
                    "status": "COMPLETED",
                },
                "schemafieldindex_v2": {
                    "requiresDataBackfill": True,
                    "status": "COMPLETED",
                },
            }
        )
        result = executor.validate(_scenario(tc=105, min_real_reindex_count=2), ctx)
        assert result.status == "PASS"

    def test_fails_when_fewer_than_min(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={
                "dashboardindex_v2": {
                    "requiresDataBackfill": True,
                    "status": "COMPLETED",
                },
                "schemafieldindex_v2": {
                    "requiresDataBackfill": False,  # not flagged
                    "status": "COMPLETED",
                },
            }
        )
        result = executor.validate(_scenario(tc=105, min_real_reindex_count=2), ctx)
        assert result.status == "FAIL"
        assert "Only 1" in (result.actual_result or "")

    def test_fails_when_any_backfill_alias_incomplete(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        raw = self._real_stack_raw()
        raw["dashboardindex_v2"]["status"] = "IN_PROGRESS"
        ctx.upgrade_blocking = UpgradeBlockingResult(raw=raw)
        result = executor.validate(_scenario(tc=105, min_real_reindex_count=2), ctx)
        assert result.status == "FAIL"
        assert "IN_PROGRESS" in (result.actual_result or "")

    def test_skips_when_raw_empty(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw={})
        result = executor.validate(_scenario(tc=105, min_real_reindex_count=2), ctx)
        assert result.status == "SKIP"

    def test_skips_when_min_count_unset(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw=self._real_stack_raw())
        # min_real_reindex_count=None — scenario opted out.
        result = executor.validate(_scenario(tc=105), ctx)
        assert result.status == "SKIP"
        assert "not configured" in (result.actual_result or "")


class TestTC103SettingsOnlyUpdate:
    _TARGET = "datasetindex_v2"

    def _scenario_with_expected(
        self, expected: frozenset[str] | None
    ) -> ZDUTestScenario:
        return _scenario(tc=103, expected_in_place_update_indices=expected)

    def test_passes_when_expected_in_place_update_observed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices_updated_in_place=[
                "datasetindex_v2",
                "chartindex_v2",
                "dataflowindex_v2",
            ],
        )
        result = executor.validate(
            self._scenario_with_expected(frozenset({self._TARGET})), ctx
        )
        assert result.status == "PASS", result.actual_result

    def test_fails_when_expected_in_place_update_missing(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices_updated_in_place=["chartindex_v2"]
        )
        result = executor.validate(
            self._scenario_with_expected(frozenset({self._TARGET})), ctx
        )
        assert result.status == "FAIL"
        assert "datasetindex_v2" in (result.actual_result or "")

    def test_fails_when_index_also_reindexed_with_swap(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Production took BOTH paths for the expected index — that's a regression.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices_updated_in_place=["datasetindex_v2"],
            alias_swaps_observed=[("datasetindex_v2", "datasetindex_v2_new")],
        )
        result = executor.validate(
            self._scenario_with_expected(frozenset({self._TARGET})), ctx
        )
        assert result.status == "FAIL"
        assert "BOTH paths" in (result.actual_result or "")

    def test_passes_when_swap_is_for_different_index(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # dashboardindex_v2 reindexed via swap (unrelated); datasetindex_v2
        # took the in-place path as expected.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices_updated_in_place=["datasetindex_v2"],
            alias_swaps_observed=[("dashboardindex_v2", "dashboardindex_v2_new")],
        )
        result = executor.validate(
            self._scenario_with_expected(frozenset({self._TARGET})), ctx
        )
        assert result.status == "PASS"

    def test_skips_when_phase4_did_not_run(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        result = executor.validate(
            self._scenario_with_expected(frozenset({self._TARGET})), TestContext()
        )
        assert result.status == "SKIP"

    def test_skips_when_expected_unset(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices_updated_in_place=["datasetindex_v2"]
        )
        result = executor.validate(self._scenario_with_expected(None), ctx)
        assert result.status == "SKIP"
        assert "not configured" in (result.actual_result or "")


class TestTC107StateShape:
    def test_passes_when_all_required_keys_present(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        # raw is now a per-alias dict (not nested under "indicesState").
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={"dashboardindex_v2": dict(_ALL_REQUIRED_KEYS)}
        )
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_required_keys_missing(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Drop a required key (status is required; taskId is NOT in
        # _REQUIRED_INDICES_STATE_KEYS so dropping it would not fail).
        partial = {k: v for k, v in _ALL_REQUIRED_KEYS.items() if k != "status"}
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw={"dashboardindex_v2": partial})
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "FAIL"
        assert "status" in (result.actual_result or "")

    def test_taskid_not_required(self, executor: Phase1ReindexExecutor) -> None:
        # Empty-source no-op entries don't get a taskId; that must still PASS.
        no_taskid = {k: v for k, v in _ALL_REQUIRED_KEYS.items() if k != "taskId"}
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            raw={"schemafieldindex_v2": no_taskid}
        )
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "PASS"

    def test_skips_when_raw_empty(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw={})
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "SKIP"


class TestTC108ReRunAfterCompleted:
    def _ctx(
        self,
        phase6_swaps: list[tuple[str, str]],
        rerun_skipped: list[str],
        rerun_swaps: list[tuple[str, str]],
        rerun_exit_code: int = 0,
    ) -> TestContext:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(alias_swaps_observed=phase6_swaps)
        ctx.upgrade_blocking_rerun = UpgradeBlockingReRunResult(
            skip_already_done_aliases=rerun_skipped,
            rerun_alias_swaps_observed=rerun_swaps,
            rerun_exit_code=rerun_exit_code,
        )
        return ctx

    def test_passes_when_all_phase6_indices_skipped(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            phase6_swaps=[
                ("dashboardindex_v2", "dashboardindex_v2_new"),
                ("schemafieldindex_v2", ""),
                ("corpgroupindex_v2", ""),
            ],
            rerun_skipped=["dashboardindex_v2"],
            rerun_swaps=[],
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "PASS", result.actual_result

    def test_passes_when_phase6_index_absent_from_rerun(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Production opt-out path: previously-reindexed index doesn't even
        # appear in the rerun's swap capture. Acceptable.
        ctx = self._ctx(
            phase6_swaps=[("dashboardindex_v2", "dashboardindex_v2_new")],
            rerun_skipped=[],
            rerun_swaps=[],
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "PASS"

    def test_fails_when_rerun_produces_new_real_reindex(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            phase6_swaps=[("dashboardindex_v2", "dashboardindex_v2_new")],
            rerun_skipped=[],
            rerun_swaps=[("dashboardindex_v2", "dashboardindex_v2_NEWER")],
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "FAIL"
        assert "dashboardindex_v2" in (result.actual_result or "")

    def test_fails_when_rerun_exit_code_nonzero(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            phase6_swaps=[("dashboardindex_v2", "dashboardindex_v2_new")],
            rerun_skipped=["dashboardindex_v2"],
            rerun_swaps=[],
            rerun_exit_code=1,
        )
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "FAIL"
        assert "exited with code 1" in (result.actual_result or "")

    def test_skips_when_rerun_phase_did_not_run(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            alias_swaps_observed=[("dashboardindex_v2", "dashboardindex_v2_new")]
        )
        # upgrade_blocking_rerun is None
        result = executor.validate(_scenario(tc=108), ctx)
        assert result.status == "SKIP"

    def test_skips_when_phase6_baseline_missing(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Rerun ran but Phase 6 didn't capture — can't verify which aliases
        # should have been skipped.
        ctx = TestContext()
        ctx.upgrade_blocking_rerun = UpgradeBlockingReRunResult(
            skip_already_done_aliases=["dashboardindex_v2"],
            rerun_exit_code=0,
        )
        result = executor.validate(_scenario(tc=108), ctx)
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


class TestTC109DocCountPreservation:
    def _snap(self, **counts: int):
        from tests.zdu.framework.context import SnapshotT0

        return SnapshotT0(epoch_ms=0, doc_counts=dict(counts))

    def test_passes_when_all_counts_match(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.snapshot_t0 = self._snap(dashboardindex_v2=19, datasetindex_v2=0)
        ctx.snapshot_t1 = self._snap(dashboardindex_v2=19, datasetindex_v2=0)
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_count_decreases(self, executor: Phase1ReindexExecutor) -> None:
        # t1 < t0 → real doc loss → FAIL
        ctx = TestContext()
        ctx.snapshot_t0 = self._snap(dashboardindex_v2=19, datasetindex_v2=0)
        ctx.snapshot_t1 = self._snap(dashboardindex_v2=14, datasetindex_v2=0)
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "FAIL"
        assert "dashboardindex_v2" in (result.actual_result or "")
        assert "19" in (result.actual_result or "")
        assert "14" in (result.actual_result or "")

    def test_passes_when_count_increases(self, executor: Phase1ReindexExecutor) -> None:
        # t1 > t0 is allowed — ES caught up from seed-lag, not a bug.
        ctx = TestContext()
        ctx.snapshot_t0 = self._snap(dashboardindex_v2=0, datasetindex_v2=0)
        ctx.snapshot_t1 = self._snap(dashboardindex_v2=19, datasetindex_v2=3)
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "PASS"

    def test_fails_when_index_missing_at_t1(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.snapshot_t0 = self._snap(dashboardindex_v2=19)
        ctx.snapshot_t1 = self._snap()  # alias dropped
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "FAIL"
        assert "dashboardindex_v2" in (result.actual_result or "")

    def test_skips_when_t0_missing(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.snapshot_t1 = self._snap(dashboardindex_v2=19)
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "SKIP"
        assert "snapshot_t0" in (result.actual_result or "")

    def test_skips_when_t1_missing(self, executor: Phase1ReindexExecutor) -> None:
        ctx = TestContext()
        ctx.snapshot_t0 = self._snap(dashboardindex_v2=19)
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "SKIP"
        assert "snapshot_t1" in (result.actual_result or "")


class TestParseFlatIndicesState:
    """Smoke tests for the MySQL flat-key parser in upgrade_blocking."""

    def test_unflattens_real_stack_shape(self) -> None:
        from tests.zdu.framework.phases.upgrade_blocking import (
            _parse_flat_indices_state,
        )

        flat = {
            "dashboardindex_v2.status": "COMPLETED",
            "dashboardindex_v2.taskId": "qetpkfhWTsuRKO7V2iCr8Q:7258",
            "dashboardindex_v2.nextIndexName": "dashboardindex_v2_v1_5_0rc1-0",
            "dashboardindex_v2.sourceDocCount": "19",
            "dashboardindex_v2.requiresDataBackfill": "true",
            "dashboardindex_v2.reindexStartTime": "1779009124653",
            "dashboardindex_v2.reindexCompleteTime": "1779009124816",
            "schemafieldindex_v2.status": "COMPLETED",
            "schemafieldindex_v2.sourceDocCount": "0",
            "schemafieldindex_v2.requiresDataBackfill": "true",
            "schemafieldindex_v2.reindexStartTime": "1779009124216",
            "schemafieldindex_v2.reindexCompleteTime": "1779009124414",
            "schemafieldindex_v2.nextIndexName": "schemafieldindex_v2_v1_5_0rc1-0",
        }
        out = _parse_flat_indices_state(flat)
        assert set(out.keys()) == {"dashboardindex_v2", "schemafieldindex_v2"}
        # Numeric coercion
        assert out["dashboardindex_v2"]["sourceDocCount"] == 19
        assert out["schemafieldindex_v2"]["sourceDocCount"] == 0
        # Boolean coercion
        assert out["dashboardindex_v2"]["requiresDataBackfill"] is True
        # String values pass through
        assert out["dashboardindex_v2"]["status"] == "COMPLETED"
        # Empty-source aliases legitimately lack taskId
        assert "taskId" not in out["schemafieldindex_v2"]

    def test_skips_top_level_keys_without_dots(self) -> None:
        from tests.zdu.framework.phases.upgrade_blocking import (
            _parse_flat_indices_state,
        )

        flat = {
            "completed": "true",  # spurious top-level — skipped
            "dashboardindex_v2.status": "COMPLETED",
        }
        out = _parse_flat_indices_state(flat)
        assert out == {"dashboardindex_v2": {"status": "COMPLETED"}}
