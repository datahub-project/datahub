"""Per-connector ingestion migrations.

A connector author declares one-time metadata migrations (a stable id + the
migration code) via ``StatefulIngestionSourceBase.get_migrations()``. Before
ingestion, any migration not yet recorded in the ledger runs (when enabled), so
metadata already in DataHub is reshaped after a breaking connector change.

The ledger of applied migration ids rides the existing stateful-ingestion
checkpoint, keyed per pipeline. Migrations must be idempotent (see design spec).
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol, Set

from packaging.version import InvalidVersion, Version

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    JobId,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    DatahubIngestionRunSummaryClass,
)

logger = logging.getLogger(__name__)

# run(graph, report, dry_run) -> None: select the affected URNs and mutate them.
MigrationFn = Callable[[DataHubGraph, SourceReport, bool], None]

MIGRATIONS_JOB_ID = JobId("ingestion_migrations")


@dataclass
class Migration:
    """A one-time metadata migration shipped by a connector.

    ``run`` is a plain callable (not a method) so authors can pass a lambda or a
    module-level function. It must be idempotent — the per-pipeline ledger scope
    means a different pipeline over the same data can re-trigger it.
    """

    # Recommend a timestamp prefix (e.g. "20260722-lowercase-urns"): the id is the
    # run-ordering key, so global and per-connector migrations interleave by when
    # the fix was written. Stable and permanent — never reuse or renumber.
    id: str
    description: str
    run: MigrationFn
    # Optional version window [introduced_in, fixed_in) describing the releases
    # whose output is broken. The migration only needs to run for data produced by
    # a version inside the window:
    #   - introduced_in: first version that produced the broken shape (inclusive);
    #     data from *before* it was never affected, so it is skipped.
    #   - fixed_in: first version whose output is already correct (exclusive);
    #     data from it onward is already right, so it is skipped.
    # Either bound may be omitted for a one-sided gate. Both omitted => gate on the
    # id ledger only. The gate is a coarse skip; converter.should_convert is the
    # fine-grained guard that never rewrites data that doesn't match.
    introduced_in: Optional[str] = None
    fixed_in: Optional[str] = None


class MigrationConfig(ConfigModel):
    enabled: bool = False
    dry_run: bool = False
    fail_on_pending: bool = False
    # Re-run every migration even if the ledger/version gate would skip it
    # (e.g. to re-apply after a fix). Still requires enabled=true to mutate.
    force: bool = False


class MigrationCheckpointState(CheckpointStateBase):
    applied: List[str] = []
    # The source/CLI version of the last enabled run, refreshed every run. Used by
    # the optional [introduced_in, fixed_in) version window to tell whether the
    # data currently in DataHub was produced by an affected version.
    last_source_version: Optional[str] = None


def _version(v: Optional[str]) -> Optional[Version]:
    if not v:
        return None
    try:
        return Version(v)
    except InvalidVersion:
        return None


def _needs_migration(
    introduced_in: Optional[str],
    fixed_in: Optional[str],
    last_version: Optional[str],
) -> bool:
    """Whether data produced by ``last_version`` falls in [introduced_in, fixed_in).

    Unknown/unparseable ``last_version`` => True (apply; ``should_convert`` is the
    real guard). ``< introduced_in`` => never affected. ``>= fixed_in`` => already
    correct. Either bound may be absent for a one-sided window.
    """
    v = _version(last_version)
    if v is None:
        return True
    low = _version(introduced_in)
    if low is not None and v < low:
        return False
    high = _version(fixed_in)
    if high is not None and v >= high:
        return False
    return True


# --- Global migrations: fixes that apply across many connectors -----------------
# Register a factory that receives the running source and returns a Migration
# scoped to it (so its `run` can target that source's platform/instance). The
# framework merges these into every source's get_migrations() list; the existing
# per-pipeline ledger gates each, so no global ledger or cross-pipeline lock is
# needed — the migration simply runs once per pipeline, like a connector-declared
# one.
MigrationFactory = Callable[[Any], Migration]

_GLOBAL_MIGRATIONS: List[MigrationFactory] = []


def register_migration(factory: MigrationFactory) -> None:
    """Register a migration that applies to every stateful source."""
    _GLOBAL_MIGRATIONS.append(factory)


def collect_migrations(
    source_migrations: List[Migration], source: Any
) -> List[Migration]:
    """Merge a source's own migrations with the globally-registered ones.

    Global factories are given the ``source`` so they can scope themselves. A
    source-declared id wins over a global one on a clash.
    """
    merged = list(source_migrations)
    seen = {m.id for m in merged}
    for factory in _GLOBAL_MIGRATIONS:
        migration = factory(source)
        if migration.id not in seen:
            merged.append(migration)
            seen.add(migration.id)
    return merged


class MigrationPendingError(Exception):
    """Raised when migrations are pending but disabled and fail_on_pending is set."""


class MigrationLedger(Protocol):
    def read_applied(self) -> Set[str]: ...

    def read_last_version(self) -> Optional[str]: ...

    def record(self, migration_id: str, source_version: Optional[str]) -> None: ...

    def touch_version(self, source_version: Optional[str]) -> None:
        """Record the current run's version without adding an applied id."""
        ...


class CheckpointMigrationLedger:
    """Applied-migration ledger backed by the stateful-ingestion checkpoint.

    Stored as a ``datahubIngestionCheckpoint`` aspect on the migrations DataJob
    URN, keyed by pipeline name. Each record is committed immediately (its own
    emit) so a later ingestion failure cannot lose the record.
    """

    def __init__(self, graph: DataHubGraph, pipeline_name: str, run_id: str) -> None:
        self.graph = graph
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._urn = IngestionCheckpointingProviderBase.get_data_job_urn(
            "datahub", pipeline_name, MIGRATIONS_JOB_ID
        )
        state = self._read()
        self._applied: Set[str] = set(state.applied) if state else set()
        self._last_version: Optional[str] = state.last_source_version if state else None

    def _read(self) -> Optional[MigrationCheckpointState]:
        aspect = self.graph.get_latest_timeseries_value(
            entity_urn=self._urn,
            aspect_type=DatahubIngestionCheckpointClass,
            filter_criteria_map={"pipelineName": self.pipeline_name},
        )
        checkpoint = Checkpoint.create_from_checkpoint_aspect(
            job_name=MIGRATIONS_JOB_ID,
            checkpoint_aspect=aspect,
            state_class=MigrationCheckpointState,
        )
        if checkpoint is not None and isinstance(
            checkpoint.state, MigrationCheckpointState
        ):
            return checkpoint.state
        return None

    def read_applied(self) -> Set[str]:
        return set(self._applied)

    def read_last_version(self) -> Optional[str]:
        return self._last_version

    def record(self, migration_id: str, source_version: Optional[str]) -> None:
        self._applied.add(migration_id)
        self._last_version = source_version
        self._commit(source_version)

    def touch_version(self, source_version: Optional[str]) -> None:
        # Refresh the recorded version for the next run's window check, without
        # adding an applied id. No-op when it hasn't changed (record() just set it).
        if source_version == self._last_version:
            return
        self._last_version = source_version
        self._commit(source_version)

    def seed_last_version(self, source_version: Optional[str]) -> None:
        # In-memory only (no emit): supply a version for *this* run's window check
        # when our own ledger has none yet (e.g. the first run — see
        # read_last_ingestion_version). Never overrides a version we recorded.
        if self._last_version is None:
            self._last_version = source_version

    def _commit(self, source_version: Optional[str]) -> None:
        checkpoint: Checkpoint[MigrationCheckpointState] = Checkpoint(
            job_name=MIGRATIONS_JOB_ID,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=MigrationCheckpointState(
                applied=sorted(self._applied), last_source_version=source_version
            ),
        )
        aspect = checkpoint.to_checkpoint_aspect(max_allowed_state_size=2**24)
        if aspect is not None:
            # Soft-delete the bookkeeping DataJob so it stays out of search/UI
            # (the timeseries checkpoint aspect remains readable), matching the
            # built-in checkpointing provider.
            self.graph.soft_delete_entity(self._urn)
            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(entityUrn=self._urn, aspect=aspect)
            )


def read_last_ingestion_version(
    graph: DataHubGraph, pipeline_config: Any
) -> Optional[str]:
    """Best-effort: the CLI/source version of this pipeline's last ingestion run.

    Reads ``DatahubIngestionRunSummary.softwareVersion`` — a per-pipeline timeseries
    aspect emitted by the run-summary reporting provider (default-on for the
    datahub-rest sink / managed ingestion). This lets the ``[introduced_in,
    fixed_in)`` window fire on the *first* migrations run, before our own ledger
    has recorded a version. Returns None (→ treat as unknown) on any problem,
    including when reporting never ran. The URN scheme mirrors
    ``DatahubIngestionRunSummaryProvider.generate_unique_key``.
    """
    if pipeline_config is None:
        return None
    try:
        key = {"type": pipeline_config.source.type}
        if pipeline_config.pipeline_name:
            key["pipeline_name"] = pipeline_config.pipeline_name
        source_config = pipeline_config.source.config or {}
        if "platform_instance" in source_config:
            key["platform_instance"] = source_config["platform_instance"]
        urn = f"urn:li:dataHubIngestionSource:cli-{datahub_guid(key)}"
        summary = graph.get_latest_timeseries_value(
            entity_urn=urn,
            aspect_type=DatahubIngestionRunSummaryClass,
            filter_criteria_map={},
        )
        return summary.softwareVersion if summary is not None else None
    except Exception as e:
        logger.debug(f"Could not read last ingestion version for the window gate: {e}")
        return None


def run_migrations(
    migrations: List[Migration],
    ledger: MigrationLedger,
    graph: DataHubGraph,
    report: SourceReport,
    config: MigrationConfig,
    current_version: str,
) -> None:
    """Apply pending migrations (in id order) before ingestion.

    Pending = migrations whose id is not in the ledger and whose optional
    ``[introduced_in, fixed_in)`` window still covers the version that produced the
    existing data — unless ``config.force`` is set, which re-runs every migration
    regardless. Pending migrations run sorted by id, so a global fix and a
    connector fix interleave by their (recommended timestamp) ids. If disabled,
    pending migrations are reported (and optionally fail the run) but not applied.
    On an enabled run the ledger's version is refreshed to ``current_version`` so
    the next run's window check knows what produced the data.
    """
    applied = ledger.read_applied()
    last_version = ledger.read_last_version()
    pending = sorted(
        (
            m
            for m in migrations
            if config.force
            or (
                m.id not in applied
                and _needs_migration(m.introduced_in, m.fixed_in, last_version)
            )
        ),
        key=lambda m: m.id,
    )

    if pending and not config.enabled:
        message = (
            f"{len(pending)} pending ingestion migration(s): "
            f"{[m.id for m in pending]}. "
            "Set stateful_ingestion.migrations.enabled=true to apply them."
        )
        report.warning(message=message, title="Pending ingestion migrations")
        if config.fail_on_pending:
            raise MigrationPendingError(message)
        return

    for migration in pending:
        logger.info(
            f"Running ingestion migration {migration.id}: {migration.description}"
        )
        migration.run(graph, report, config.dry_run)
        if not config.dry_run:
            ledger.record(migration.id, current_version)

    # Remember the version that produced the data for the next run's window check.
    if config.enabled and not config.dry_run:
        ledger.touch_version(current_version)
