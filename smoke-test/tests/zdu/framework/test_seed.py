"""Unit tests for SeedPhase IO-pool direct-MySQL seed path (Plan F-5)."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.seed import SeedPhase


def _build_phase() -> tuple[SeedPhase, MagicMock, MagicMock]:
    executor = MagicMock()
    datahub = MagicMock()
    docker = MagicMock()
    mysql = MagicMock()
    phase = SeedPhase(
        executor=executor,
        scenarios=[],
        datahub=datahub,
        docker=docker,
        mysql=mysql,
    )
    return phase, datahub, mysql


class TestSeedIOPoolViaMysql:
    def test_seeds_io_pool_directly_via_mysql_not_api(self) -> None:
        phase, datahub, mysql = _build_phase()
        ctx = TestContext()
        phase._seed_io_pool_via_mysql(ctx)
        # 5 IO-pool URNs (zdu-io-pool-0..4)
        assert mysql.upsert_aspect_raw.call_count == 5
        # GMS API must NOT be invoked for IO-pool seeding.
        assert datahub.ingest_mcp.call_count == 0
        # Every call uses systemmetadata='{}' (no schemaVersion key)
        for call in mysql.upsert_aspect_raw.call_args_list:
            kwargs = call.kwargs
            assert kwargs.get("systemmetadata", "{}") == "{}"
        # ctx.io_pool_entities populated with 5 SeededEntity rows
        assert len(ctx.io_pool_entities) == 5
        urns = {e.urn for e in ctx.io_pool_entities}
        assert urns == {f"urn:li:dashboard:(test,zdu-io-pool-{i})" for i in range(5)}

    def test_io_pool_seed_aspect_is_embed(self) -> None:
        phase, _datahub, mysql = _build_phase()
        ctx = TestContext()
        phase._seed_io_pool_via_mysql(ctx)
        for call in mysql.upsert_aspect_raw.call_args_list:
            kw = call.kwargs
            args = call.args
            # aspect may be passed positionally or as keyword
            assert kw.get("aspect") == "embed" or "embed" in args


class TestRegisterReferencedPlatforms:
    """Master GMS rejects dataset writes whose referenced platform doesn't
    exist. The seed phase must register `urn:li:dataPlatform:test` via
    dataPlatformInfo before any scenario seed touches a dataset URN.
    """

    def test_registers_test_platform_via_ingest_mcp(self) -> None:
        phase, datahub, _mysql = _build_phase()
        phase._register_referenced_platforms()
        # One call per platform in _PLATFORMS_TO_REGISTER (currently just 'test').
        assert datahub.ingest_mcp.call_count == 1
        call_kw = datahub.ingest_mcp.call_args_list[0].kwargs
        assert call_kw["urn"] == "urn:li:dataPlatform:test"
        assert call_kw["aspect_name"] == "dataPlatformInfo"
        assert call_kw["data"]["name"] == "test"
        assert call_kw["data"]["type"] == "OTHERS"
        assert call_kw["data"]["datasetNameDelimiter"] == "."

    def test_swallows_ingest_failure(self) -> None:
        # Registration failure shouldn't abort SeedPhase — per-scenario writes
        # will surface the underlying error with full per-URN context.
        phase, datahub, _mysql = _build_phase()
        datahub.ingest_mcp.side_effect = RuntimeError("boom")
        phase._register_referenced_platforms()  # no raise
        assert datahub.ingest_mcp.call_count == 1


class TestWaitForESDrain:
    """G19c-v2: scenario seed writes are async to ES (MCL → MAE → doc). If
    Phase 6's reindex runs before they drain, post-reindex docs come up
    missing (TC-015 failure mode). SeedPhase polls ES for each scenario URN
    until present or timeout, failing the phase loudly rather than letting
    downstream phases silently see a stale index.
    """

    @staticmethod
    def _scenario(tc: int, entity_type: str = "dashboard"):
        s = MagicMock()
        s.tc_number = tc
        s.entity_type = entity_type
        s.name = f"zdu-tc-{tc}"
        return s

    def test_returns_empty_when_no_es_client(self) -> None:
        # ES wiring is optional — when omitted (e.g., legacy callers), the
        # drain step short-circuits and returns no pending URNs.
        phase = SeedPhase(
            executor=MagicMock(),
            scenarios=[self._scenario(1)],
            datahub=MagicMock(),
            docker=MagicMock(),
            mysql=MagicMock(),
            es=None,
        )
        assert phase._wait_for_es_drain() == []

    def test_returns_empty_when_no_scenarios_have_known_alias(self) -> None:
        # If every scenario's entity_type lacks a mapping (e.g., chart in a
        # future suite), drain skips silently and returns empty.
        phase = SeedPhase(
            executor=MagicMock(),
            scenarios=[self._scenario(1, entity_type="unknown_entity")],
            datahub=MagicMock(),
            docker=MagicMock(),
            mysql=MagicMock(),
            es=MagicMock(),
        )
        assert phase._wait_for_es_drain() == []

    def test_returns_empty_when_all_urns_present_immediately(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = {"urn": "any"}  # always found
        phase = SeedPhase(
            executor=MagicMock(),
            scenarios=[self._scenario(1), self._scenario(2)],
            datahub=MagicMock(),
            docker=MagicMock(),
            mysql=MagicMock(),
            es=es,
            drain_timeout_s=2,
            drain_poll_interval_s=0.01,
        )
        pending = phase._wait_for_es_drain()
        assert pending == []
        # One get_doc per scenario in the first pass — no retry needed.
        assert es.get_doc.call_count == 2

    def test_returns_pending_urns_on_timeout(self) -> None:
        es = MagicMock()
        es.get_doc.return_value = None  # never found
        phase = SeedPhase(
            executor=MagicMock(),
            scenarios=[self._scenario(15), self._scenario(20)],
            datahub=MagicMock(),
            docker=MagicMock(),
            mysql=MagicMock(),
            es=es,
            drain_timeout_s=0,  # immediate timeout
            drain_poll_interval_s=0.01,
        )
        pending = phase._wait_for_es_drain()
        # Both URNs reported as pending.
        assert len(pending) == 2
        assert "zdu-tc-15" in pending[0]
        assert "zdu-tc-20" in pending[1]

    def test_partial_drain_resolves_on_retry(self) -> None:
        # First pass: tc-1 found, tc-2 missing.
        # Second pass: tc-2 found.
        es = MagicMock()
        responses = iter(
            [
                {"urn": "1"},  # tc-1 pass 1: found
                None,  # tc-2 pass 1: missing
                {"urn": "2"},  # tc-2 pass 2: found
            ]
        )
        es.get_doc.side_effect = lambda *a, **kw: next(responses)
        phase = SeedPhase(
            executor=MagicMock(),
            scenarios=[self._scenario(1), self._scenario(2)],
            datahub=MagicMock(),
            docker=MagicMock(),
            mysql=MagicMock(),
            es=es,
            drain_timeout_s=5,
            drain_poll_interval_s=0.01,
        )
        pending = phase._wait_for_es_drain()
        assert pending == []
        assert es.get_doc.call_count == 3
