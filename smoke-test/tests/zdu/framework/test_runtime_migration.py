"""Unit tests for RuntimeMigrationPhase — uses mocked DataHubClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import SeededEntity, TestContext
from tests.zdu.framework.datahub_client import AspectResponse
from tests.zdu.framework.phases.runtime_migration import RuntimeMigrationPhase


def _seeded(i: int, expected_version: int = 4) -> SeededEntity:
    return SeededEntity(
        urn=f"urn:li:dashboard:(test,zdu-tc-{i})",
        aspect_name="embed",
        tc_number=i,
        seeded_data={"renderUrl": f"http://seed/{i}"},
        expected_schema_version=expected_version,
        validator=lambda d: True,
    )


def _aspect(version: int) -> AspectResponse:
    return AspectResponse(
        data={"renderUrl": "http://x/x"},
        system_metadata={"schemaVersion": version},
    )


@pytest.fixture
def datahub() -> MagicMock:
    m = MagicMock()
    m.get_aspect.return_value = _aspect(4)
    return m


@pytest.fixture
def phase(datahub: MagicMock) -> RuntimeMigrationPhase:
    return RuntimeMigrationPhase(datahub=datahub, n_write_probes=2, n_read_probes=3)


class TestRuntimeMigrationPhase:
    def test_reads_n_seeded_entities_and_records_probes(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[_seeded(i) for i in range(5)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert len(ctx.runtime_migration.read_probes) == 3
        assert ctx.runtime_migration.passed_read_count == 3
        # All read probes captured the right (urn, observed_version, expected_version)
        for probe in ctx.runtime_migration.read_probes:
            assert probe.observed_version == probe.expected_version == 4
            assert probe.mode == "read"

    def test_writes_n_fresh_entities_and_verifies_persisted_version(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[_seeded(0)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert len(ctx.runtime_migration.write_probes) == 2
        assert ctx.runtime_migration.passed_write_count == 2
        # n_write_probes calls to ingest_mcp + (n_read_probes + n_write_probes) get_aspect
        assert datahub.ingest_mcp.call_count == 2
        assert (
            datahub.get_aspect.call_count == 1 + 2
        )  # 1 read probe (clamped to 1 seed) + 2 write read-backs

    def test_read_probe_records_mismatch_without_failing_phase(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        # GMS returns v2 instead of expected v4 for ALL get_aspect calls —
        # both the read probes and the write-probe read-backs see v2,
        # so neither mode passes. Phase still reports passed; Phase 10 is the verdict.
        datahub.get_aspect.return_value = _aspect(2)
        ctx = TestContext(seeded_entities=[_seeded(0), _seeded(1), _seeded(2)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.passed_read_count == 0
        assert ctx.runtime_migration.passed_write_count == 0
        assert all(
            p.observed_version == 2 and p.expected_version == 4
            for p in ctx.runtime_migration.read_probes
        )
        assert all(
            p.observed_version == 2 and p.expected_version == 4
            for p in ctx.runtime_migration.write_probes
        )

    def test_write_probe_records_error_when_ingest_fails(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        datahub.ingest_mcp.side_effect = RuntimeError("GMS unavailable")
        ctx = TestContext(seeded_entities=[_seeded(0)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.passed_write_count == 0
        assert all(
            "GMS unavailable" in (p.error or "")
            for p in ctx.runtime_migration.write_probes
        )

    def test_no_seeded_entities_skips_read_probes(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.read_probes == []
        # Write probes still run — disjoint URN namespace.
        assert len(ctx.runtime_migration.write_probes) == 2
