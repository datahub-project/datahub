"""Unit tests for the per-connector ingestion migration runner."""

from typing import Iterable, List, Set, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.state.ingestion_migration import (
    Migration,
    MigrationConfig,
    MigrationPendingError,
    run_migrations,
)


class FakeLedger:
    def __init__(self, applied: Iterable[str] = ()):
        self._applied: Set[str] = set(applied)
        self.recorded: List[str] = []

    def read_applied(self) -> Set[str]:
        return set(self._applied)

    def record(self, migration_id: str) -> None:
        self._applied.add(migration_id)
        self.recorded.append(migration_id)


def _mig(mid: str) -> Tuple[Migration, MagicMock]:
    run = MagicMock()
    return Migration(id=mid, description=mid, run=run), run


def test_runs_only_pending_in_order_and_records():
    (a, ra), (b, rb), (c, rc) = _mig("a"), _mig("b"), _mig("c")
    ledger = FakeLedger({"a"})
    graph, report = MagicMock(), MagicMock()
    run_migrations([a, b, c], ledger, graph, report, MigrationConfig(enabled=True))
    ra.assert_not_called()
    rb.assert_called_once_with(graph, report, False)
    rc.assert_called_once_with(graph, report, False)
    assert ledger.recorded == ["b", "c"]


def test_declared_order_preserved():
    migs = [_mig("c")[0], _mig("a")[0], _mig("b")[0]]
    ledger = FakeLedger()
    run_migrations(
        migs, ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True)
    )
    assert ledger.recorded == ["c", "a", "b"]


def test_disabled_warns_and_skips():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    report = MagicMock()
    run_migrations([a], ledger, MagicMock(), report, MigrationConfig(enabled=False))
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
        )


def test_dry_run_runs_but_does_not_record():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    graph, report = MagicMock(), MagicMock()
    run_migrations(
        [a], ledger, graph, report, MigrationConfig(enabled=True, dry_run=True)
    )
    ra.assert_called_once_with(graph, report, True)
    assert ledger.recorded == []


def test_no_pending_is_noop():
    (a, ra) = _mig("a")
    ledger = FakeLedger({"a"})
    report = MagicMock()
    run_migrations([a], ledger, MagicMock(), report, MigrationConfig(enabled=True))
    ra.assert_not_called()
    assert ledger.recorded == []
    report.warning.assert_not_called()
