"""Unit tests for SnapshotT0Phase — uses mocked ES + MySQL clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.snapshot_t0 import SnapshotT0Phase


@pytest.fixture
def es() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    return MagicMock()


@pytest.fixture
def phase(es: MagicMock, mysql: MagicMock) -> SnapshotT0Phase:
    return SnapshotT0Phase(
        es=es,
        mysql=mysql,
        aspects_under_test=["embed", "globalTags"],
        upgrade_id_to_check=None,
    )


class TestSnapshotT0PhaseHappyPath:
    def test_captures_aliases_doc_counts_and_aspects(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        # Pre-Phase-1 state: indices exist as physical indices with no aliases yet.
        es.list_indices.return_value = [
            "datasetindex_v2",
            "dashboardindex_v2",
            "datasetindex_v2_1714000000",  # already-reindexed style
        ]
        # Post-Phase-1 state: "datasetindex_v2" is now an alias pointing
        # at the timestamped physical index. The other names are physical
        # indices with no aliases of their own.
        es.get_alias_targets.side_effect = lambda alias: (
            ["datasetindex_v2_1714000000"] if alias == "datasetindex_v2" else []
        )

        def doc_count(idx: str) -> int:
            return {
                "datasetindex_v2": 100,
                "dashboardindex_v2": 50,
                "datasetindex_v2_1714000000": 100,
            }[idx]

        es.get_doc_count.side_effect = doc_count
        mysql.count_aspects_by_schema_version.side_effect = lambda aspect: {
            "embed": {None: 5, 4: 1184},
            "globalTags": {2: 19},
        }[aspect]

        ctx = TestContext()
        result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.snapshot_t0 is not None
        snap = ctx.snapshot_t0
        # Doc counts captured for every physical index returned by list_indices.
        assert snap.doc_counts == {
            "datasetindex_v2": 100,
            "dashboardindex_v2": 50,
            "datasetindex_v2_1714000000": 100,
        }
        # Aspect histograms captured for both configured aspects.
        assert snap.aspects_by_version == {
            "embed": {None: 5, 4: 1184},
            "globalTags": {2: 19},
        }
        # Alias mapping captured (only the alias name appears as a key;
        # the physical-index names that returned [] do not).
        assert snap.indices == {
            "datasetindex_v2": ["datasetindex_v2_1714000000"],
        }
        # T0 is a positive epoch_ms.
        assert snap.epoch_ms > 0


class TestSnapshotT0PhaseDefensiveErrors:
    def test_es_failure_marks_phase_failed_but_does_not_crash(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.side_effect = ConnectionError("boom")
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "boom" in (result.error or "")

    def test_one_aspect_query_failure_does_not_drop_other_aspects(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.return_value = ["datasetindex_v2"]
        es.get_doc_count.return_value = 0

        def aspect_query(aspect: str) -> dict:
            if aspect == "embed":
                raise RuntimeError("mysql blip")
            return {2: 7}

        mysql.count_aspects_by_schema_version.side_effect = aspect_query
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.snapshot_t0 is not None
        # embed missing from the histogram (skipped on error); globalTags present.
        assert "embed" not in ctx.snapshot_t0.aspects_by_version
        assert ctx.snapshot_t0.aspects_by_version["globalTags"] == {2: 7}

    def test_one_index_doc_count_failure_does_not_drop_other_indices(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.return_value = ["a_index_v2", "b_index_v2"]

        def doc_count(idx: str) -> int:
            if idx == "a_index_v2":
                raise ConnectionError("blip")
            return 42

        es.get_doc_count.side_effect = doc_count
        mysql.count_aspects_by_schema_version.return_value = {}
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        # a_index_v2 omitted, b_index_v2 captured.
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.doc_counts == {"b_index_v2": 42}


class TestSnapshotT0PhaseUpgradeResultCheck:
    def test_no_upgrade_id_means_no_check(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es,
            mysql=mysql,
            aspects_under_test=[],
            upgrade_id_to_check=None,
        )
        es.list_indices.return_value = []
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is False
        # MySQL not asked about upgrade results because upgrade_id_to_check is None.
        mysql.get_upgrade_result.assert_not_called()

    def test_upgrade_id_present_sets_flag_true(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es,
            mysql=mysql,
            aspects_under_test=[],
            upgrade_id_to_check="system-update-blocking",
        )
        es.list_indices.return_value = []
        mysql.get_upgrade_result.return_value = {"state": "SUCCEEDED"}
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is True
        mysql.get_upgrade_result.assert_called_once_with("system-update-blocking")

    def test_upgrade_id_absent_sets_flag_false(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es,
            mysql=mysql,
            aspects_under_test=[],
            upgrade_id_to_check="system-update-blocking",
        )
        es.list_indices.return_value = []
        mysql.get_upgrade_result.return_value = None
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is False
