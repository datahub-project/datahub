"""Unit tests for InjectTrafficDualPhase — uses mocked DataHub/MySQL/ES clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import IndexState, TestContext, UpgradeBlockingResult
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_dual import InjectTrafficDualPhase


@pytest.fixture
def datahub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.get_aspect_raw.return_value = EbeanAspectV2Row(
        urn="placeholder",
        aspect="embed",
        version=0,
        metadata="{}",
        systemmetadata="{}",
        createdon="2026-01-01 00:00:00",
        createdby="urn:li:corpuser:datahub",
    )
    return m


@pytest.fixture
def es() -> MagicMock:
    m = MagicMock()
    m.get_doc.return_value = {"urn": "placeholder"}
    return m


@pytest.fixture
def phase(
    datahub: MagicMock, mysql: MagicMock, es: MagicMock
) -> InjectTrafficDualPhase:
    return InjectTrafficDualPhase(
        datahub=datahub,
        mysql=mysql,
        es=es,
        n_dual_writes=3,
    )


def _ctx_with_distinct_indices() -> TestContext:
    ctx = TestContext()
    ctx.upgrade_blocking = UpgradeBlockingResult(
        indices=[
            IndexState(
                alias="dashboardindex_v2",
                old_backing_index_name="dashboardindex_v2_old",
                next_index_name="dashboardindex_v2_new",
                source_doc_count=100,
                status="COMPLETED",
            )
        ]
    )
    return ctx


class TestInjectTrafficDualPhaseHappyPath:
    def test_writes_n_entities_and_records_dual_write_urns(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES returns the doc in BOTH old and new indices (correct dual-write).
        es.get_doc.return_value = {"urn": "x"}

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert len(ctx.dual_write_urns) == 3
        assert all(
            u.startswith("urn:li:dashboard:(test,zdu-dual-")
            for u in ctx.dual_write_urns
        )
        assert datahub.ingest_mcp.call_count == 3
        assert mysql.get_aspect_raw.call_count == 3
        # 3 URNs × 2 indices (old + new) = 6 get_doc calls
        assert es.get_doc.call_count == 6
        # No warnings — both indices have the doc
        assert result.details.get("dual_write_warnings") == []

    def test_old_index_missing_doc_emits_warning(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says doc is in NEW only, NOT in OLD — dual-write fan-out failed.
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_new" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed"  # warnings don't fail the phase
        warnings = result.details.get("dual_write_warnings") or []
        assert len(warnings) == 3  # one per URN
        assert all("missing from OLD index" in w for w in warnings)

    def test_new_index_missing_doc_emits_warning(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says doc is in OLD only, NOT in NEW — dual-write fan-out failed.
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_old" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed"
        warnings = result.details.get("dual_write_warnings") or []
        assert len(warnings) == 3
        assert all("missing from NEW index" in w for w in warnings)

    def test_degenerate_single_image_case_skips_index_distinction(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices=[
                IndexState(
                    alias="dashboardindex_v2",
                    old_backing_index_name="dashboardindex_v2",
                    next_index_name="dashboardindex_v2",
                    source_doc_count=100,
                    status="COMPLETED",
                )
            ]
        )
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.dual_write_urns) == 3
        # Index distinction skipped — at most n_dual_writes get_doc calls.
        assert es.get_doc.call_count <= 3

    def test_no_upgrade_blocking_state_skips_index_checks_entirely(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.dual_write_urns) == 3
        es.get_doc.assert_not_called()


class TestInjectTrafficDualPhaseFailures:
    def test_mysql_missing_one_urn_fails_phase(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        rows = [
            EbeanAspectV2Row(
                urn=f"urn-{i}",
                aspect="embed",
                version=0,
                metadata="{}",
                systemmetadata="{}",
                createdon="2026-01-01 00:00:00",
                createdby="urn:li:corpuser:datahub",
            )
            for i in range(2)
        ] + [None]
        mysql.get_aspect_raw.side_effect = rows

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "missing from MySQL" in (result.error or "")
        # Records full ingested list on MySQL failure (all 3 ingests succeeded)
        assert len(ctx.dual_write_urns) == 3
        # Same list must also be surfaced in details for the JSON report
        assert result.details.get("dual_write_urns") == ctx.dual_write_urns

    def test_ingest_failure_fails_phase_with_partial_dual_write_urns(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # First 2 ingests succeed, third raises
        datahub.ingest_mcp.side_effect = [None, None, RuntimeError("GMS unavailable")]

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "GMS unavailable" in (result.error or "")
        # Partial list: 2 successful ingests recorded
        assert len(ctx.dual_write_urns) == 2
        assert ctx.dual_write_urns[0] == "urn:li:dashboard:(test,zdu-dual-0)"
        assert ctx.dual_write_urns[1] == "urn:li:dashboard:(test,zdu-dual-1)"
        # Same partial list must also be surfaced in details for the JSON report
        assert result.details.get("dual_write_urns") == ctx.dual_write_urns
