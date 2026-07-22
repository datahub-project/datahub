"""Unit tests for the per-connector ingestion migration runner."""

from typing import Iterable, List, Optional, Set, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.state.ingestion_migration import (
    Migration,
    MigrationConfig,
    MigrationPendingError,
    run_migrations,
)

VERSION = "1.5.0"


class FakeLedger:
    def __init__(self, applied: Iterable[str] = (), last_version: Optional[str] = None):
        self._applied: Set[str] = set(applied)
        self._last_version = last_version
        self.recorded: List[str] = []
        self.recorded_versions: List[Optional[str]] = []

    def read_applied(self) -> Set[str]:
        return set(self._applied)

    def read_last_version(self) -> Optional[str]:
        return self._last_version

    def record(self, migration_id: str, source_version: Optional[str]) -> None:
        self._applied.add(migration_id)
        self.recorded.append(migration_id)
        self.recorded_versions.append(source_version)


def _mig(mid: str, apply_before: Optional[str] = None) -> Tuple[Migration, MagicMock]:
    run = MagicMock()
    return (
        Migration(id=mid, description=mid, run=run, apply_before=apply_before),
        run,
    )


def test_runs_only_pending_in_order_and_records():
    (a, ra), (b, rb), (c, rc) = _mig("a"), _mig("b"), _mig("c")
    ledger = FakeLedger({"a"})
    graph, report = MagicMock(), MagicMock()
    run_migrations(
        [a, b, c], ledger, graph, report, MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()
    rb.assert_called_once_with(graph, report, False)
    rc.assert_called_once_with(graph, report, False)
    assert ledger.recorded == ["b", "c"]
    assert ledger.recorded_versions == [VERSION, VERSION]


def test_declared_order_preserved():
    migs = [_mig("c")[0], _mig("a")[0], _mig("b")[0]]
    ledger = FakeLedger()
    run_migrations(
        migs, ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    assert ledger.recorded == ["c", "a", "b"]


def test_disabled_warns_and_skips():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    report = MagicMock()
    run_migrations(
        [a], ledger, MagicMock(), report, MigrationConfig(enabled=False), VERSION
    )
    ra.assert_not_called()
    assert ledger.recorded == []
    report.warning.assert_called_once()


def test_disabled_fail_on_pending_raises():
    (a, _ra) = _mig("a")
    with pytest.raises(MigrationPendingError):
        run_migrations(
            [a],
            FakeLedger(),
            MagicMock(),
            MagicMock(),
            MigrationConfig(enabled=False, fail_on_pending=True),
            VERSION,
        )


def test_dry_run_runs_but_does_not_record():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    graph, report = MagicMock(), MagicMock()
    run_migrations(
        [a], ledger, graph, report, MigrationConfig(enabled=True, dry_run=True), VERSION
    )
    ra.assert_called_once_with(graph, report, True)
    assert ledger.recorded == []


def test_no_pending_is_noop():
    (a, ra) = _mig("a")
    ledger = FakeLedger({"a"})
    report = MagicMock()
    run_migrations(
        [a], ledger, MagicMock(), report, MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()
    assert ledger.recorded == []
    report.warning.assert_not_called()


# --- version gating (apply_before) ---


def test_version_gate_skips_when_last_version_at_or_above_apply_before():
    (a, ra) = _mig("a", apply_before="1.2.0")
    ledger = FakeLedger(last_version="1.3.0")  # data already produced by >= 1.2.0
    report = MagicMock()
    run_migrations(
        [a], ledger, MagicMock(), report, MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()
    assert ledger.recorded == []
    report.warning.assert_not_called()


def test_version_gate_runs_when_last_version_below_apply_before():
    (a, ra) = _mig("a", apply_before="1.2.0")
    ledger = FakeLedger(last_version="1.1.0")
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_called_once()
    assert ledger.recorded == ["a"]


def test_version_gate_runs_when_no_last_version():
    (a, ra) = _mig("a", apply_before="1.2.0")
    ledger = FakeLedger()  # never migrated before
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_called_once()


def test_no_apply_before_ignores_version():
    (a, ra) = _mig("a")  # no apply_before
    ledger = FakeLedger(last_version="9.9.9")
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_called_once()


def test_force_reruns_applied_and_version_gated():
    (a, ra) = _mig("a")  # already applied
    (b, rb) = _mig("b", apply_before="1.2.0")  # version-superseded
    ledger = FakeLedger({"a"}, last_version="9.9.9")
    run_migrations(
        [a, b],
        ledger,
        MagicMock(),
        MagicMock(),
        MigrationConfig(enabled=True, force=True),
        VERSION,
    )
    ra.assert_called_once()
    rb.assert_called_once()
    assert set(ledger.recorded) == {"a", "b"}


def test_checkpoint_ledger_hides_bookkeeping_datajob():
    from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
        IngestionCheckpointingProviderBase,
    )
    from datahub.ingestion.source.state.ingestion_migration import (
        MIGRATIONS_JOB_ID,
        CheckpointMigrationLedger,
    )

    graph = MagicMock()
    graph.get_latest_timeseries_value.return_value = None
    ledger = CheckpointMigrationLedger(graph, "p", "r")
    ledger.record("m1", "1.0.0")

    urn = IngestionCheckpointingProviderBase.get_data_job_urn(
        "datahub", "p", MIGRATIONS_JOB_ID
    )
    graph.soft_delete_entity.assert_called_with(urn)
    graph.emit_mcp.assert_called_once()
