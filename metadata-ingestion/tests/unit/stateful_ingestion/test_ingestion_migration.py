"""Unit tests for the per-connector ingestion migration runner."""

from typing import Iterable, Optional, Set
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.state.ingestion_migration import (
    _GLOBAL_MIGRATIONS,
    Migration,
    MigrationConfig,
    MigrationPendingError,
    _needs_migration,
    collect_migrations,
    register_migration,
    run_migrations,
)

VERSION = "1.5.0"


class FakeLedger:
    def __init__(self, applied: Iterable[str] = (), last_version: Optional[str] = None):
        self._applied: Set[str] = set(applied)
        self._last_version = last_version
        self.recorded: list = []
        self.recorded_versions: list = []
        self.touched: list = []

    def read_applied(self) -> Set[str]:
        return set(self._applied)

    def read_last_version(self) -> Optional[str]:
        return self._last_version

    def record(self, migration_id: str, source_version: Optional[str]) -> None:
        self._applied.add(migration_id)
        self._last_version = source_version
        self.recorded.append(migration_id)
        self.recorded_versions.append(source_version)

    def touch_version(self, source_version: Optional[str]) -> None:
        self._last_version = source_version
        self.touched.append(source_version)


def _mig(mid, introduced_in=None, fixed_in=None):
    run = MagicMock()
    return (
        Migration(
            id=mid,
            description=mid,
            run=run,
            introduced_in=introduced_in,
            fixed_in=fixed_in,
        ),
        run,
    )


@pytest.fixture
def clean_registry():
    saved = list(_GLOBAL_MIGRATIONS)
    _GLOBAL_MIGRATIONS.clear()
    yield
    _GLOBAL_MIGRATIONS[:] = saved


# --- _needs_migration (version window [introduced_in, fixed_in)) ---


def test_needs_migration_unknown_version_applies():
    assert _needs_migration("1.2.0", "1.4.0", None) is True


def test_needs_migration_predates_bug_skips():
    assert _needs_migration("1.2.0", "1.4.0", "1.1.0") is False


def test_needs_migration_in_window_applies():
    assert _needs_migration("1.2.0", "1.4.0", "1.3.0") is True
    assert _needs_migration("1.2.0", "1.4.0", "1.2.0") is True  # inclusive lower


def test_needs_migration_already_fixed_skips():
    assert _needs_migration("1.2.0", "1.4.0", "1.4.0") is False  # exclusive upper
    assert _needs_migration("1.2.0", "1.4.0", "1.5.0") is False


def test_needs_migration_open_bounds():
    assert _needs_migration(None, "1.4.0", "1.0.0") is True  # upper bound only
    assert _needs_migration(None, "1.4.0", "1.4.0") is False
    assert _needs_migration("1.2.0", None, "9.9.9") is True  # lower bound only
    assert _needs_migration("1.2.0", None, "1.1.0") is False
    assert _needs_migration(None, None, "1.1.0") is True  # no gate


def test_needs_migration_invalid_version_applies():
    assert _needs_migration("1.2.0", "1.4.0", "not-a-version") is True


# --- run_migrations ---


def test_runs_only_pending_and_records():
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


def test_runs_in_id_order_not_declared_order():
    # declared out of order; the id drives run order (recommend timestamp-prefixed ids)
    migs = [_mig("20260901-c")[0], _mig("20260101-a")[0], _mig("20260401-b")[0]]
    ledger = FakeLedger()
    run_migrations(
        migs, ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    assert ledger.recorded == ["20260101-a", "20260401-b", "20260901-c"]


def test_disabled_warns_skips_and_does_not_touch():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    report = MagicMock()
    run_migrations(
        [a], ledger, MagicMock(), report, MigrationConfig(enabled=False), VERSION
    )
    ra.assert_not_called()
    assert ledger.recorded == []
    assert ledger.touched == []
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


def test_dry_run_runs_but_does_not_record_or_touch():
    (a, ra) = _mig("a")
    ledger = FakeLedger()
    graph, report = MagicMock(), MagicMock()
    run_migrations(
        [a], ledger, graph, report, MigrationConfig(enabled=True, dry_run=True), VERSION
    )
    ra.assert_called_once_with(graph, report, True)
    assert ledger.recorded == []
    assert ledger.touched == []


def test_enabled_touches_version_when_nothing_pending():
    (a, ra) = _mig("a")
    ledger = FakeLedger({"a"})  # already applied → nothing pending
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()
    assert ledger.touched == [VERSION]


def test_version_window_skips_when_data_predates_bug():
    (a, ra) = _mig("a", introduced_in="1.2.0", fixed_in="1.4.0")
    ledger = FakeLedger(last_version="1.1.0")
    report = MagicMock()
    run_migrations(
        [a], ledger, MagicMock(), report, MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()
    assert ledger.recorded == []
    report.warning.assert_not_called()


def test_version_window_runs_when_data_in_window():
    (a, ra) = _mig("a", introduced_in="1.2.0", fixed_in="1.4.0")
    ledger = FakeLedger(last_version="1.3.0")
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_called_once()
    assert ledger.recorded == ["a"]


def test_version_window_skips_when_already_fixed():
    (a, ra) = _mig("a", introduced_in="1.2.0", fixed_in="1.4.0")
    ledger = FakeLedger(last_version="1.5.0")
    run_migrations(
        [a], ledger, MagicMock(), MagicMock(), MigrationConfig(enabled=True), VERSION
    )
    ra.assert_not_called()


def test_force_reruns_applied_and_out_of_window():
    (a, ra) = _mig("a")  # already applied
    (b, rb) = _mig("b", fixed_in="1.2.0")  # data at 9.9.9 → out of window
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


# --- global migration registry ---


def test_collect_merges_global_migrations_scoped_to_source(clean_registry):
    source = MagicMock()
    seen = []

    def factory(src):
        seen.append(src)
        return Migration(id="global-x", description="x", run=MagicMock())

    register_migration(factory)
    merged = collect_migrations([_mig("a")[0]], source)
    assert [m.id for m in merged] == ["a", "global-x"]
    assert seen == [source]  # factory receives the source, for scoping


def test_collect_source_declaration_wins_on_id_clash(clean_registry):
    def factory(src):
        return Migration(id="dup", description="global", run=MagicMock())

    register_migration(factory)
    source_mig = Migration(id="dup", description="source", run=MagicMock())
    merged = collect_migrations([source_mig], MagicMock())
    assert len(merged) == 1
    assert merged[0].description == "source"


def test_collect_no_global_registered_returns_source_only(clean_registry):
    source_mig = Migration(id="a", description="a", run=MagicMock())
    assert collect_migrations([source_mig], MagicMock()) == [source_mig]


# --- checkpoint ledger ---


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


def test_checkpoint_ledger_touch_version_persists_without_new_id():
    from datahub.ingestion.source.state.ingestion_migration import (
        CheckpointMigrationLedger,
    )

    graph = MagicMock()
    graph.get_latest_timeseries_value.return_value = None
    ledger = CheckpointMigrationLedger(graph, "p", "r")
    ledger.touch_version("2.0.0")
    assert ledger.read_last_version() == "2.0.0"
    assert ledger.read_applied() == set()
    graph.emit_mcp.assert_called_once()


def test_checkpoint_ledger_seed_last_version_in_memory_only():
    from datahub.ingestion.source.state.ingestion_migration import (
        CheckpointMigrationLedger,
    )

    graph = MagicMock()
    graph.get_latest_timeseries_value.return_value = None
    ledger = CheckpointMigrationLedger(graph, "p", "r")
    ledger.seed_last_version("1.1.0")
    assert ledger.read_last_version() == "1.1.0"
    graph.emit_mcp.assert_not_called()  # seeding does not persist

    ledger._last_version = "2.0.0"  # once we have a real version, seed is ignored
    ledger.seed_last_version("1.1.0")
    assert ledger.read_last_version() == "2.0.0"


# --- seeding last_version from DatahubIngestionRunSummary (first-run coverage) ---


def _pipeline_config(source_type="mysql", pipeline_name="p", platform_instance=None):
    pc = MagicMock()
    pc.source.type = source_type
    pc.pipeline_name = pipeline_name
    pc.source.config = (
        {"platform_instance": platform_instance} if platform_instance else {}
    )
    return pc


def test_read_last_ingestion_version_from_run_summary():
    from datahub.ingestion.source.state.ingestion_migration import (
        read_last_ingestion_version,
    )

    graph = MagicMock()
    summary = MagicMock()
    summary.softwareVersion = "1.3.0"
    graph.get_latest_timeseries_value.return_value = summary
    assert read_last_ingestion_version(graph, _pipeline_config()) == "1.3.0"
    _, kwargs = graph.get_latest_timeseries_value.call_args
    assert kwargs["entity_urn"].startswith("urn:li:dataHubIngestionSource:cli-")


def test_read_last_ingestion_version_none_when_no_config():
    from datahub.ingestion.source.state.ingestion_migration import (
        read_last_ingestion_version,
    )

    assert read_last_ingestion_version(MagicMock(), None) is None


def test_read_last_ingestion_version_none_when_no_summary():
    from datahub.ingestion.source.state.ingestion_migration import (
        read_last_ingestion_version,
    )

    graph = MagicMock()
    graph.get_latest_timeseries_value.return_value = None
    assert read_last_ingestion_version(graph, _pipeline_config()) is None


def test_read_last_ingestion_version_best_effort_on_error():
    from datahub.ingestion.source.state.ingestion_migration import (
        read_last_ingestion_version,
    )

    graph = MagicMock()
    graph.get_latest_timeseries_value.side_effect = RuntimeError("boom")
    assert read_last_ingestion_version(graph, _pipeline_config()) is None
