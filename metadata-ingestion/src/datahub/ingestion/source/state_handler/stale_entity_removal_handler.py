import logging
from typing import Iterable, Optional, Type, cast

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.ingestion_job_state_provider import JobId
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.stale_entity_checkpoint_base import (
    StaleEntityCheckpointStateBase,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import ChangeTypeClass, StatusClass

logger: logging.Logger = logging.getLogger(__name__)


class StaleEntityRemovalHandler:
    """
    The stateful ingestion helper class that handles stale entity removal.
    This contains the generic logic for all sources that need to support stale entity removal for all the states
    derived from StaleEntityCheckpointStateBase. This uses the template method pattern on CRTP based derived state
    class hierarchies.
    """

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: Optional[StatefulStaleMetadataRemovalConfig],
        state_type_class: Type[StaleEntityCheckpointStateBase],
        job_id: JobId,
    ):
        self.config = config
        self.source = source
        self.state_type_class = state_type_class
        self.job_id = job_id
        self.is_checkpointing_enabled = (
            source.is_stateful_ingestion_configured()
            and config
            and config.remove_stale_metadata
        )

    def _create_soft_delete_workunit(self, urn: str, type: str) -> MetadataWorkUnit:

        logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
        mcp = MetadataChangeProposalWrapper(
            entityType=type,
            entityUrn=urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="status",
            aspect=StatusClass(removed=True),
        )
        wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
        report = self.source.get_report()
        assert isinstance(report, StaleEntityRemovalSourceReport)
        report.report_workunit(wu)
        report.report_stale_entity_soft_deleted(urn)
        return wu

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.is_checkpointing_enabled:
            return
        logger.debug("Checking for stale entity removal.")
        last_checkpoint: Optional[Checkpoint] = self.source.get_last_checkpoint(
            self.job_id, self.state_type_class
        )
        if not last_checkpoint:
            return
        cur_checkpoint = self.source.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        # Get the underlying states
        last_checkpoint_state = cast(
            StaleEntityCheckpointStateBase, last_checkpoint.state
        )
        cur_checkpoint_state = cast(
            StaleEntityCheckpointStateBase, cur_checkpoint.state
        )
        for type in self.state_type_class.get_supported_types():
            for urn in last_checkpoint_state.get_urns_not_in(
                type=type, other_checkpoint=cur_checkpoint_state
            ):
                yield self._create_soft_delete_workunit(urn, type)

    def add_entity_to_state(self, type: str, urn: str) -> None:
        if not self.is_checkpointing_enabled:
            return
        cur_checkpoint = self.source.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(StaleEntityCheckpointStateBase, cur_checkpoint.state)
        cur_state.add_checkpoint_urn(type=type, urn=urn)
