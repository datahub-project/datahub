"""Unit tests for FailureBundleWriter — uses tmp_path + mocked clients."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import (
    IOObservation,
    IOWriteResult,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
    ValidationResult,
)
from tests.zdu.framework.failure_bundle import FailureBundleWriter
from tests.zdu.framework.phases.base import PhaseResult
from tests.zdu.framework.runner import ZDUReport


def _failed_report() -> ZDUReport:
    cfg = MagicMock()
    cfg.gms_service = "datahub-gms-debug"
    rpt = ZDUReport(config=cfg)
    rpt.phase_results.append(
        PhaseResult(
            phase_name="upgrade_nonblocking",
            status="failed",
            started_at=datetime.utcnow(),
            duration_s=10.0,
            error="something broke",
            details={},
        )
    )
    return rpt


def _passing_report() -> ZDUReport:
    cfg = MagicMock()
    cfg.gms_service = "datahub-gms-debug"
    rpt = ZDUReport(config=cfg)
    rpt.phase_results.append(
        PhaseResult(
            phase_name="discovery",
            status="passed",
            started_at=datetime.utcnow(),
            duration_s=1.0,
            details={},
        )
    )
    return rpt


@pytest.fixture
def writer(
    tmp_path: Path,
) -> tuple[FailureBundleWriter, MagicMock, MagicMock, MagicMock]:
    docker = MagicMock()
    es = MagicMock()
    mysql = MagicMock()
    docker.get_service_logs.return_value = "GMS log line\n"
    es.cat_indices.return_value = "indices\n"
    es.cat_aliases.return_value = "aliases\n"
    mysql.dump_zdu_aspects_csv.return_value = "urn,aspect\n"
    return (
        FailureBundleWriter(docker=docker, es=es, mysql=mysql, build_dir=tmp_path),
        docker,
        es,
        mysql,
    )


class TestFailureBundleWriter:
    def test_skips_when_no_failures(self, writer) -> None:
        bundle, docker, es, mysql = writer
        path = bundle.write(_passing_report(), TestContext())
        assert path is None
        docker.get_service_logs.assert_not_called()
        es.cat_indices.assert_not_called()
        mysql.dump_zdu_aspects_csv.assert_not_called()

    def test_writes_bundle_on_phase_failure(self, writer, tmp_path) -> None:
        bundle, docker, es, mysql = writer
        path = bundle.write(_failed_report(), TestContext())
        assert path is not None
        assert path.parent == tmp_path
        assert path.name.startswith("zdu-failure-")
        # Required files present
        assert (path / "summary.json").exists()
        assert (path / "compose-logs").is_dir()
        assert (path / "es-cat-indices.txt").exists()
        assert (path / "es-cat-aliases.txt").exists()
        assert (path / "upgrade-result.json").exists()
        assert (path / "mysql-aspects.csv").exists()
        assert (path / "reader-writer-events.jsonl").exists()
        # Body contents survive
        assert (
            "GMS log line"
            in (path / "compose-logs" / "datahub-gms-debug.log").read_text()
        )
        assert "indices" in (path / "es-cat-indices.txt").read_text()
        assert "urn,aspect" in (path / "mysql-aspects.csv").read_text()

    def test_writes_bundle_on_scenario_failure(self, writer) -> None:
        bundle, *_ = writer
        rpt = _passing_report()
        rpt.scenario_results = [
            ValidationResult(
                tc_number=320,
                name="Read path in-memory only",
                status="FAIL",
                expected_to_fail=False,
                actual_result="expected v4 got v2",
                failure_reason="schema mismatch",
            )
        ]
        path = bundle.write(rpt, TestContext())
        assert path is not None
        # Failed scenario appears in summary
        import json as _json

        summary = _json.loads((path / "summary.json").read_text())
        failed_scenarios = [
            s for s in summary.get("failed_scenarios", []) if s.get("tc") == 320
        ]
        assert len(failed_scenarios) == 1

    def test_upgrade_result_includes_blocking_and_nonblocking(self, writer) -> None:
        bundle, *_ = writer
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw={"blocking": True})
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(raw={"nonblocking": True})
        path = bundle.write(_failed_report(), ctx)
        assert path is not None
        import json as _json

        body = _json.loads((path / "upgrade-result.json").read_text())
        assert body["blocking"] == {"blocking": True}
        assert body["nonblocking"] == {"nonblocking": True}

    def test_reader_writer_events_dump_observations_and_writes(self, writer) -> None:
        bundle, *_ = writer
        ctx = TestContext()
        ts = datetime.utcnow()
        ctx.io_observations = [
            IOObservation(
                worker="reader-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                aspect_name="embed",
                observed_version=4,
                expected_version=4,
                timestamp=ts,
            )
        ]
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4,
                expected_version=4,
                passed=True,
                timestamp=ts,
            )
        ]
        path = bundle.write(_failed_report(), ctx)
        assert path is not None
        lines = (path / "reader-writer-events.jsonl").read_text().splitlines()
        assert len(lines) == 2
        assert any('"reader-0"' in line for line in lines)
        assert any('"writer-0"' in line for line in lines)

    def test_continues_on_per_artifact_failure(self, writer) -> None:
        bundle, docker, es, mysql = writer
        # Make ES dump raise — the bundle should still be created.
        es.cat_indices.side_effect = RuntimeError("ES unreachable")
        path = bundle.write(_failed_report(), TestContext())
        assert path is not None
        # ES file is empty / present-but-empty, but MySQL CSV still works.
        assert (path / "mysql-aspects.csv").exists()
        # The cat-indices.txt either doesn't exist or is empty — both acceptable.
        if (path / "es-cat-indices.txt").exists():
            assert (path / "es-cat-indices.txt").read_text() == ""
