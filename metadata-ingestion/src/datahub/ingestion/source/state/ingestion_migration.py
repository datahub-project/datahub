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
from typing import Callable, List, Optional, Protocol, Set

from packaging.version import InvalidVersion, Version

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    JobId,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

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

    id: str
    description: str
    run: MigrationFn
    # Optional version gate: the version whose output is already correct (usually
    # the release that ships this migration). The migration is only needed for
    # data produced by *earlier* versions, so it is skipped once the pipeline's
    # last run was already at/after `apply_before` (newer code emits the correct
    # shape). None => gate on the id ledger only.
    apply_before: Optional[str] = None


class MigrationConfig(ConfigModel):
    enabled: bool = False
    dry_run: bool = False
    fail_on_pending: bool = False
    # Re-run every migration even if the ledger/version gate would skip it
    # (e.g. to re-apply after a fix). Still requires enabled=true to mutate.
    force: bool = False


class MigrationCheckpointState(CheckpointStateBase):
    applied: List[str] = []
    # The source/CLI version recorded when a migration was last applied. Used by
    # the optional `apply_before` version gate.
    last_source_version: Optional[str] = None


def _is_superseded(apply_before: Optional[str], last_version: Optional[str]) -> bool:
    """True when the last run was already at/after `apply_before` (data already correct)."""
    if not apply_before or not last_version:
        return False
    try:
        return Version(last_version) >= Version(apply_before)
    except InvalidVersion:
        return False


class MigrationPendingError(Exception):
    """Raised when migrations are pending but disabled and fail_on_pending is set."""


class MigrationLedger(Protocol):
    def read_applied(self) -> Set[str]: ...

    def read_last_version(self) -> Optional[str]: ...

    def record(self, migration_id: str, source_version: Optional[str]) -> None: ...


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
            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(entityUrn=self._urn, aspect=aspect)
            )


def run_migrations(
    migrations: List[Migration],
    ledger: MigrationLedger,
    graph: DataHubGraph,
    report: SourceReport,
    config: MigrationConfig,
    current_version: str,
) -> None:
    """Apply pending migrations (declared order) before ingestion.

    Pending = migrations whose id is not in the ledger and whose optional
    ``apply_before`` version is not already superseded by the last applied version
    (i.e. still needed for data produced by an earlier version) — unless
    ``config.force`` is set, which re-runs every migration regardless. If
    disabled, pending migrations are reported (and optionally fail the run) but
    not applied. ``current_version`` is recorded on each apply.
    """
    applied = ledger.read_applied()
    last_version = ledger.read_last_version()
    pending = [
        m
        for m in migrations
        if config.force
        or (m.id not in applied and not _is_superseded(m.apply_before, last_version))
    ]
    if not pending:
        return

    if not config.enabled:
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
