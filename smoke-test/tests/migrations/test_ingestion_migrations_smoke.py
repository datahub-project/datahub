"""Smoke test: per-connector ingestion migrations run before ingestion (live GMS)."""

import logging
from random import randint
from typing import List

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.state.ingestion_migration import (
    MIGRATIONS_JOB_ID,
    CheckpointMigrationLedger,
    Migration,
    MigrationConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, with_test_retry

logger = logging.getLogger(__name__)

_suffix = randint(10, 100000)
PIPELINE = f"migrations-smoke-{_suffix}"
MARKER_DS = make_dataset_urn("snowflake", f"migsmoke_{_suffix}.tbl")
MIGRATION_ID = "smoke-0001-tag"
TAG = "urn:li:tag:migrations_smoke"


def _tag_marker(graph: DataHubGraph, report: SourceReport, dry_run: bool) -> None:
    if dry_run:
        return
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=MARKER_DS,
            aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=TAG)]),
        )
    )


class _MigratingSource(StatefulIngestionSourceBase):
    def get_migrations(self) -> List[Migration]:
        return [Migration(id=MIGRATION_ID, description="tag a marker", run=_tag_marker)]

    def get_workunits_internal(self):
        return []

    def get_report(self) -> SourceReport:
        return self.report


def _make_source(graph: DataHubGraph, enabled: bool) -> _MigratingSource:
    config = StatefulIngestionConfigBase[StatefulIngestionConfig](
        stateful_ingestion=StatefulIngestionConfig(
            enabled=True, migrations=MigrationConfig(enabled=enabled)
        )
    )
    ctx = PipelineContext(
        run_id="migrations-smoke", graph=graph, pipeline_name=PIPELINE
    )
    return _MigratingSource(config, ctx)


def test_migration_runs_before_ingestion_and_is_recorded(
    graph_client: DataHubGraph,
) -> None:
    ledger_urn = IngestionCheckpointingProviderBase.get_data_job_urn(
        "datahub", PIPELINE, MIGRATIONS_JOB_ID
    )
    delete_urns(graph_client, [MARKER_DS, ledger_urn])
    wait_for_writes_to_sync()
    try:
        # get_workunits() triggers the pre-ingestion migration hook.
        list(_make_source(graph_client, enabled=True).get_workunits())
        wait_for_writes_to_sync()

        @with_test_retry()
        def check_applied() -> None:
            tags = graph_client.get_aspect(MARKER_DS, GlobalTagsClass)
            assert tags is not None and any(t.tag == TAG for t in tags.tags)
            applied = CheckpointMigrationLedger(
                graph_client, PIPELINE, "verify"
            ).read_applied()
            assert MIGRATION_ID in applied

        check_applied()
    finally:
        delete_urns(graph_client, [MARKER_DS, ledger_urn])
        wait_for_writes_to_sync()
