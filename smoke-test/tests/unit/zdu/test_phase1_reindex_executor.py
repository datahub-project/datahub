"""Unit tests for Phase1ReindexExecutor."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import (
    IndexState,
    SettingsOnlyUpgradeResult,
    SnapshotT0,
    TestContext,
    UpgradeBlockingReRunResult,
    UpgradeBlockingResult,
)
from tests.zdu.framework.phase1_reindex_executor import Phase1ReindexExecutor
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.PLATFORM)


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
    min_real_reindex_count: int | None = None,
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
        min_real_reindex_count=min_real_reindex_count,
    )


# Blocking-phase indicesState entries as a Search V3 stack writes them: a
# populated index is copied by _reindex, an empty one is planned as a fill.
_REINDEX_ENTRY = {
    "status": "COMPLETED",
    "nextIndexName": "datasetindex_v3_v1_9_0rc1-1_1795277536286",
    "sourceDocCount": 3,
    "requiresDataBackfill": True,
    "reindexStartTime": 1795277536286,
    "reindexCompleteTime": 1795277536440,
    "taskId": "_OzrOBMmSRSEzelFPpF2Ew:7612",
}
_FILL_ENTRY = {
    "status": "BACKFILL_PENDING",
    "nextIndexName": "tagindex_v3_v1_9_0rc1-1_1795277529771",
    "sourceDocCount": 0,
    "requiresDataBackfill": False,
    "reindexStartTime": 1795277529771,
    "backfillTargetIndex": "tagindex_v3_v1_9_0rc1-1_1795277529771",
    "backfillStartTime": 1795277529771,
}


def _blocking(raw: dict[str, dict]) -> UpgradeBlockingResult:
    """Build the blocking result the way UpgradeBlockingPhase does from MySQL."""
    return UpgradeBlockingResult(
        indices=[
            IndexState(
                alias=alias,
                next_index_name=entry.get("nextIndexName"),
                source_doc_count=entry.get("sourceDocCount", 0),
                task_id=entry.get("taskId"),
                status=entry.get("status", "UNKNOWN"),
            )
            for alias, entry in raw.items()
        ],
        raw=raw,
    )


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


class TestSkipReasonDispatch:
    def test_skip_reason_without_xfail_returns_skip(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        scen = _scenario(tc=103, skip_reason="nothing to observe on V3")
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"
        assert result.actual_result == "nothing to observe on V3"


class TestUnknownTC:
    def test_unknown_tc_returns_skip(self, executor: Phase1ReindexExecutor) -> None:
        scen = _scenario(tc=199)  # not in the registered range
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"


class TestTC101PopulatedIndicesReindexed:
    def _ctx(self, t0_counts: dict[str, int], raw: dict[str, dict]) -> TestContext:
        ctx = TestContext()
        ctx.snapshot_t0 = SnapshotT0(epoch_ms=0, alias_doc_counts=t0_counts)
        ctx.upgrade_blocking = _blocking(raw)
        return ctx

    def test_passes_when_reindexed_set_matches_populated_at_t0(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            {"datasetindex_v3": 3, "tagindex_v3": 0},
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY},
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_populated_index_not_reindexed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Docs at T0 but the upgrade planned it like an empty index.
        ctx = self._ctx(
            {"datasetindex_v3": 3, "tagindex_v3": 5},
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY},
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "FAIL"
        assert "tagindex_v3" in (result.actual_result or "")

    def test_passes_when_index_populated_after_t0_is_reindexed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Background GMS writers can add an index's first docs after T0 but
        # before the blocking upgrade reaches it; copying them is correct.
        ctx = self._ctx(
            {"datasetindex_v3": 0},
            {"datasetindex_v3": _REINDEX_ENTRY},
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "PASS", result.actual_result
        assert "datasetindex_v3" in (result.actual_result or "")

    def test_fails_when_reindex_task_runs_on_empty_source(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        empty_copy = dict(_REINDEX_ENTRY, sourceDocCount=0)
        ctx = self._ctx({"datasetindex_v3": 0}, {"datasetindex_v3": empty_copy})
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "FAIL"
        assert "datasetindex_v3" in (result.actual_result or "")

    def test_fails_when_populated_index_not_processed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Docs at T0, but the upgrade never touched the index at all.
        ctx = self._ctx(
            {"datasetindex_v3": 3, "chartindex_v3": 2},
            {"datasetindex_v3": _REINDEX_ENTRY},
        )
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "FAIL"
        assert "chartindex_v3" in (result.actual_result or "")

    def test_skips_without_t0_alias_counts(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"datasetindex_v3": _REINDEX_ENTRY})
        result = executor.validate(_scenario(tc=101), ctx)
        assert result.status == "SKIP"


class TestTC102NoV2Indices:
    def _ctx(self, t1_indices: list[str]) -> TestContext:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking(
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY}
        )
        ctx.snapshot_t1 = SnapshotT0(epoch_ms=0, index_names=t1_indices)
        return ctx

    def test_passes_with_only_v3_entity_indices(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            [
                "datasetindex_v3_v1_9_0rc1-1_1795277536286",
                "tagindex_v3_v1_9_0rc1-1_1795277529771",
                "graph_service_v1",
                "system_metadata_service_v1",
            ]
        )
        result = executor.validate(_scenario(tc=102), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_v2_entity_index_present(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = self._ctx(
            ["datasetindex_v3_v1_9_0rc1-1_1795277536286", "datasetindex_v2_1795277536"]
        )
        result = executor.validate(_scenario(tc=102), ctx)
        assert result.status == "FAIL"
        assert "datasetindex_v2_1795277536" in (result.actual_result or "")

    def test_skips_without_t1_listing(self, executor: Phase1ReindexExecutor) -> None:
        result = executor.validate(_scenario(tc=102), self._ctx([]))
        assert result.status == "SKIP"


class TestTC103SettingsOnlyUpdate:
    def _ctx(self, run: SettingsOnlyUpgradeResult) -> TestContext:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking(
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY}
        )
        ctx.upgrade_settings_only = run
        return ctx

    def _run(self, **kwargs: object) -> SettingsOnlyUpgradeResult:
        run = SettingsOnlyUpgradeResult(
            desired_refresh_interval="7s",
            settings_updated={"datasetindex_v3": "7s"},
            es_refresh_intervals={"datasetindex_v3": "7s"},
            exit_code=0,
        )
        for key, value in kwargs.items():
            setattr(run, key, value)
        return run

    def test_passes_when_populated_index_updated_in_place(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        result = executor.validate(_scenario(tc=103), self._ctx(self._run()))
        assert result.status == "PASS", result.actual_result

    def test_fails_when_settings_change_triggers_reindex(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        run = self._run(reindex_tasks=["datasetindex_v3"])
        result = executor.validate(_scenario(tc=103), self._ctx(run))
        assert result.status == "FAIL"
        assert "reindex_tasks=['datasetindex_v3']" in (result.actual_result or "")

    def test_fails_when_es_keeps_old_value(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # The log claims the update, but ES still serves the old interval.
        run = self._run(es_refresh_intervals={"datasetindex_v3": "3s"})
        result = executor.validate(_scenario(tc=103), self._ctx(run))
        assert result.status == "FAIL"
        assert "datasetindex_v3=3s" in (result.actual_result or "")

    def test_skips_when_settings_phase_did_not_run(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"datasetindex_v3": _REINDEX_ENTRY})
        result = executor.validate(_scenario(tc=103), ctx)
        assert result.status == "SKIP"


class TestParseSettingsOnlyLine:
    def test_records_settings_update_and_reindex_task(self) -> None:
        from tests.zdu.framework.phases.upgrade_settings_only import (
            parse_settings_only_line,
        )

        result = SettingsOnlyUpgradeResult(desired_refresh_interval="7s")
        parse_settings_only_line(
            "2026-11-21 17:05:02,101 [main] INFO  c.l.m.s.e.i.ESIndexBuilder - "
            "Updated index datasetindex_v3 settings. desired refresh_interval=7s "
            'current refresh_interval=3s settings={"index.refresh_interval":"7s"} '
            "acknowledged=true",
            result,
        )
        parse_settings_only_line(
            "2026-11-21 16:12:16,412 [main] INFO  c.l.m.s.e.i.ESIndexBuilder - "
            "Incremental reindex: submitted _reindex task _OzrOBMmSRSEzelFPpF2Ew:7612 "
            "from datasetindex_v3 to datasetindex_v3_v1_9_0rc1-1_1795277536286 "
            "(source docs: 3)",
            result,
        )
        assert result.settings_updated == {"datasetindex_v3": "7s"}
        assert result.reindex_tasks == ["datasetindex_v3"]


class TestTC104EmptySourceFill:
    def test_passes_when_empty_index_planned_as_fill(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking(
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY}
        )
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "PASS", result.actual_result
        assert "tagindex_v3" in (result.actual_result or "")

    def test_fails_when_no_empty_index_observed(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"datasetindex_v3": _REINDEX_ENTRY})
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "FAIL"

    def test_fails_when_empty_index_not_planned_as_fill(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # Empty index marked COMPLETED with no fill target: nothing will fill it.
        not_filled = {
            k: v
            for k, v in _FILL_ENTRY.items()
            if k not in ("backfillTargetIndex", "backfillStartTime")
        }
        not_filled["status"] = "COMPLETED"
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"tagindex_v3": not_filled})
        result = executor.validate(_scenario(tc=104), ctx)
        assert result.status == "FAIL"
        assert "status=COMPLETED" in (result.actual_result or "")


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


class TestTC107StateShape:
    def test_passes_with_keys_for_each_path(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking(
            {"datasetindex_v3": _REINDEX_ENTRY, "tagindex_v3": _FILL_ENTRY}
        )
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "PASS", result.actual_result

    def test_fails_when_reindex_entry_lacks_complete_time(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        partial = {
            k: v for k, v in _REINDEX_ENTRY.items() if k != "reindexCompleteTime"
        }
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"datasetindex_v3": partial})
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "FAIL"
        assert "reindexCompleteTime" in (result.actual_result or "")

    def test_fails_when_fill_entry_lacks_target(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        partial = {k: v for k, v in _FILL_ENTRY.items() if k != "backfillTargetIndex"}
        ctx = TestContext()
        ctx.upgrade_blocking = _blocking({"tagindex_v3": partial})
        result = executor.validate(_scenario(tc=107), ctx)
        assert result.status == "FAIL"
        assert "backfillTargetIndex" in (result.actual_result or "")

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

        return SnapshotT0(epoch_ms=0, alias_doc_counts=dict(counts))

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

    def test_fails_when_live_alias_lost_docs_but_retired_index_kept_them(
        self, executor: Phase1ReindexExecutor
    ) -> None:
        # V3 retires the T0 physical index; it keeps its docs while the live
        # alias serves the rebuilt one. Only the alias count shows the loss.
        ctx = TestContext()
        ctx.snapshot_t0 = SnapshotT0(
            epoch_ms=0,
            doc_counts={"datasetindex_v3_v1_9_0rc1-0_1795280860654": 3},
            alias_doc_counts={"datasetindex_v3": 3},
        )
        ctx.snapshot_t1 = SnapshotT0(
            epoch_ms=0,
            doc_counts={"datasetindex_v3_v1_9_0rc1-0_1795280860654": 3},
            alias_doc_counts={"datasetindex_v3": 1},
        )
        result = executor.validate(_scenario(tc=109), ctx)
        assert result.status == "FAIL"
        assert "datasetindex_v3" in (result.actual_result or "")

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
