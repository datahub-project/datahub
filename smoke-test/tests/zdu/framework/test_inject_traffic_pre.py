"""Unit tests for InjectTrafficPrePhase — uses mocked DataHub/MySQL/ES clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import IndexState, TestContext, UpgradeBlockingResult
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_pre import InjectTrafficPrePhase


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
def phase(datahub: MagicMock, mysql: MagicMock, es: MagicMock) -> InjectTrafficPrePhase:
    return InjectTrafficPrePhase(
        datahub=datahub,
        mysql=mysql,
        es=es,
        n_gap_writes=3,
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


class TestInjectTrafficPrePhaseHappyPath:
    def test_writes_n_entities_and_records_gap_urns(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_old" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert len(ctx.gap_urns) == 3
        assert all(
            u.startswith("urn:li:dashboard:(test,zdu-gap-") for u in ctx.gap_urns
        )
        assert datahub.ingest_mcp.call_count == 3
        assert mysql.get_aspect_raw.call_count == 3

    def test_degenerate_single_image_case_skips_index_distinction(
        self,
        phase: InjectTrafficPrePhase,
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
        assert len(ctx.gap_urns) == 3
        # Index distinction skipped — at most n_gap_writes get_doc calls
        # (a single sanity check on the first URN, OR zero).
        assert es.get_doc.call_count <= 3

    def test_no_upgrade_blocking_state_skips_index_checks_entirely(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.gap_urns) == 3
        es.get_doc.assert_not_called()


class TestInjectTrafficPrePhaseFailures:
    def test_mysql_missing_one_urn_fails_phase(
        self,
        phase: InjectTrafficPrePhase,
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

    def test_ingest_failure_fails_phase(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        datahub.ingest_mcp.side_effect = [None, None, RuntimeError("GMS unavailable")]

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "GMS unavailable" in (result.error or "")

    def test_index_distinction_failure_logs_but_does_not_fail_phase(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        es.get_doc.return_value = {"urn": "x"}  # always present in BOTH old and new
        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.gap_urns) == 3
        assert "index_distinction_warnings" in (result.details or {})

    def test_partial_ingest_records_partial_gap_urns_on_failure(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # First two ingests succeed; third raises.
        datahub.ingest_mcp.side_effect = [None, None, RuntimeError("boom")]
        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        # ctx.gap_urns reflects the partial successful ingests (URNs 0 and 1)
        assert len(ctx.gap_urns) == 2
        assert ctx.gap_urns[0] == "urn:li:dashboard:(test,zdu-gap-0)"
        assert ctx.gap_urns[1] == "urn:li:dashboard:(test,zdu-gap-1)"
        # Same partial list also surfaced in details for the JSON report
        assert result.details.get("gap_urns") == ctx.gap_urns

    def test_mysql_missing_records_full_ingested_list_on_failure(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # All three ingests succeed, but MySQL says one is missing.
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
        # All 3 URNs were ingested before the MySQL check failed
        assert len(ctx.gap_urns) == 3
