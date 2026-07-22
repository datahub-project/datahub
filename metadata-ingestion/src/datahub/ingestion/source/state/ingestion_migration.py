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
from typing import Callable, List, Protocol, Set

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


class MigrationConfig(ConfigModel):
    enabled: bool = False
    dry_run: bool = False
    fail_on_pending: bool = False


class MigrationCheckpointState(CheckpointStateBase):
    applied: List[str] = []


class MigrationPendingError(Exception):
    """Raised when migrations are pending but disabled and fail_on_pending is set."""


class MigrationLedger(Protocol):
    def read_applied(self) -> Set[str]: ...

    def record(self, migration_id: str) -> None: ...


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
        self._applied: Set[str] = self._read()

    def _read(self) -> Set[str]:
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
            return set(checkpoint.state.applied)
        return set()

    def read_applied(self) -> Set[str]:
        return set(self._applied)

    def record(self, migration_id: str) -> None:
        self._applied.add(migration_id)
        checkpoint: Checkpoint[MigrationCheckpointState] = Checkpoint(
            job_name=MIGRATIONS_JOB_ID,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=MigrationCheckpointState(applied=sorted(self._applied)),
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
) -> None:
    """Apply pending migrations (declared order) before ingestion.

    Pending = migrations whose id is not in the ledger. If disabled, pending
    migrations are reported (and optionally fail the run) but not applied.
    """
    applied = ledger.read_applied()
    pending = [m for m in migrations if m.id not in applied]
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
            ledger.record(migration.id)
