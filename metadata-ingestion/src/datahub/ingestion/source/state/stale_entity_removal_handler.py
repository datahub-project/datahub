import logging
from dataclasses import dataclass, field
from functools import partial
from typing import Dict, Iterable, Optional, Set, Type, cast

import pydantic

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.source_helpers import auto_stale_entity_removal
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.metadata.schema_classes import StatusClass
from datahub.utilities.lossy_collections import LossyList

logger: logging.Logger = logging.getLogger(__name__)


class StatefulStaleMetadataRemovalConfig(StatefulIngestionConfig):
    """
    Base specialized config for Stateful Ingestion with stale metadata removal capability.
    """

    remove_stale_metadata: bool = pydantic.Field(
        default=True,
        description="Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
    )
    fail_safe_threshold: float = pydantic.Field(
        default=100.0,
        description="Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
        le=100.0,
        ge=0.0,
        hidden_from_docs=True,
    )


@dataclass
class StaleEntityRemovalSourceReport(StatefulIngestionReport):
    soft_deleted_stale_entities: LossyList[str] = field(default_factory=LossyList)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)


class StaleEntityRemovalHandler(
    StatefulIngestionUsecaseHandlerBase["GenericCheckpointState"]
):
    """
    The stateful ingestion helper class that handles stale entity removal.
    This contains the generic logic for all sources that need to support stale entity removal for all the states
    derived from GenericCheckpointState.
    """

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
        state_type_class: Type["GenericCheckpointState"],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.source = source
        self.state_provider = source.state_provider

        self.state_type_class = state_type_class
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.stateful_ingestion_config: Optional[
            StatefulStaleMetadataRemovalConfig
        ] = config.stateful_ingestion
        self.checkpointing_enabled: bool = (
            True
            if (
                self.state_provider.is_stateful_ingestion_configured()
                and self.stateful_ingestion_config
                and self.stateful_ingestion_config.remove_stale_metadata
            )
            else False
        )
        self._job_id = self._init_job_id()
        self._urns_to_skip: Set[str] = set()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @classmethod
    def create(
        cls,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase,
        ctx: PipelineContext,
        state_type_class: Type["GenericCheckpointState"] = GenericCheckpointState,
    ) -> "StaleEntityRemovalHandler":
        return cls(source, config, state_type_class, ctx.pipeline_name, ctx.run_id)

    @property
    def workunit_processor(self):
        return partial(auto_stale_entity_removal, self)

    @classmethod
    def compute_job_id(
        cls, platform: Optional[str], unique_id: Optional[str] = None
    ) -> JobId:
        # Handle backward-compatibility for existing sources.
        backward_comp_platform_to_job_name: Dict[str, str] = {
            "bigquery": "ingest_from_bigquery_source",
            "dbt": "dbt_stateful_ingestion",
            "glue": "glue_stateful_ingestion",
            "kafka": "ingest_from_kafka_source",
            "pulsar": "ingest_from_pulsar_source",
            "snowflake": "common_ingest_from_sql_source",
        }
        if platform in backward_comp_platform_to_job_name:
            return JobId(backward_comp_platform_to_job_name[platform])

        # Default name for everything else
        job_name_suffix = "stale_entity_removal"
        # Used with set_job_id when creating multiple checkpoints in one recipe source
        # Because job_id is used as dictionary key when committing checkpoint, we have to set a new job_id
        # Refer to https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stateful_ingestion_base.py#L390
        unique_suffix = f"_{unique_id}" if unique_id else ""
        return JobId(
            f"{platform}_{job_name_suffix}{unique_suffix}"
            if platform
            else job_name_suffix
        )

    def _init_job_id(self, unique_id: Optional[str] = None) -> JobId:
        platform: Optional[str] = getattr(self.source, "platform", "default")
        return self.compute_job_id(platform, unique_id)

    def _ignore_old_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_old_state
        ):
            return True
        return False

    def _ignore_new_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_new_state
        ):
            return True
        return False

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def set_job_id(self, unique_id):
        self._job_id = self._init_job_id(unique_id)

    def is_checkpointing_enabled(self) -> bool:
        return self.checkpointing_enabled

    def create_checkpoint(self) -> Optional[Checkpoint]:
        if self.is_checkpointing_enabled() and not self._ignore_new_state():
            assert self.stateful_ingestion_config is not None
            assert self.pipeline_name is not None
            return Checkpoint(
                job_name=self.job_id,
                pipeline_name=self.pipeline_name,
                run_id=self.run_id,
                state=self.state_type_class(),
            )
        return None

    def _create_soft_delete_workunit(self, urn: str) -> MetadataWorkUnit:
        logger.info(f"Soft-deleting stale entity - {urn}")
        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=StatusClass(removed=True),
        )
        wu = MetadataWorkUnit(id=f"soft-delete-{urn}", mcp=mcp)
        report = self.source.get_report()
        assert isinstance(report, StaleEntityRemovalSourceReport)
        report.report_workunit(wu)
        report.report_stale_entity_soft_deleted(urn)
        return wu

    def add_urn_to_skip(self, urn: str) -> None:
        # We previously had bugs where sources (e.g. dbt) would add non-primary entity urns
        # to the state object. While we've (hopefully) fixed those bugs, we still have
        # old urns lingering in the previous state objects. To avoid accidentally removing
        # those, the source should call this method to track any urns that it previously was
        # erroneously adding to the state object, so that we can skip them when issuing
        # soft-deletions.
        #
        # Note that this isn't foolproof: if someone were to update acryl-datahub at the
        # same time as a non-primary entity disappeared from their ingestion, we'd
        # still issue a soft-delete for that entity. However, this shouldn't be a frequent
        # occurrence and can be fixed by re-running the primary ingestion.

        self._urns_to_skip.add(urn)

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.is_checkpointing_enabled() or self._ignore_old_state():
            return
        logger.debug("Checking for stale entity removal.")
        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, self.state_type_class
        )
        if not last_checkpoint:
            return
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        # Get the underlying states
        last_checkpoint_state: GenericCheckpointState = last_checkpoint.state
        cur_checkpoint_state = cast(GenericCheckpointState, cur_checkpoint.state)

        assert self.stateful_ingestion_config

        # Check if the entity delta is below the fail-safe threshold.
        entity_difference_percent = cur_checkpoint_state.get_percent_entities_changed(
            last_checkpoint_state
        )
        if (
            entity_difference_percent
            > self.stateful_ingestion_config.fail_safe_threshold
            # Adding this check to protect against cases where get_percent_entities_changed returns over 100%.
            # This previously happened due to a bug in the implementation, which caused this condition to be
            # triggered too frequently.
            and self.stateful_ingestion_config.fail_safe_threshold < 100.0
        ):
            # Log the failure. This would prevent the current state from getting committed.
            self.source.get_report().report_failure(
                "stale-entity-removal",
                f"Will not soft-delete entities, since we'd be deleting {entity_difference_percent:.1f}% of the existing entities. "
                f"To force a deletion, increase the value of 'stateful_ingestion.fail_safe_threshold' (currently {self.stateful_ingestion_config.fail_safe_threshold})",
            )
            return

        if self.source.get_report().events_produced == 0:
            # SUBTLE: By reporting this as a failure here, we also ensure that the
            # new (empty) state doesn't get committed.
            # TODO: Move back to using fail_safe_threshold once we're confident that we've squashed all the bugs.
            self.source.get_report().report_failure(
                "stale-entity-removal",
                "Skipping stale entity soft-deletion because the source produced no events. "
                "This is a fail-safe mechanism to prevent accidental deletion of all entities.",
            )
            return

        # If the source already had a failure, skip soft-deletion.
        # TODO: Eventually, switch this to check if anything in the pipeline had a failure so far.
        if self.source.get_report().failures:
            self.source.get_report().report_warning(
                "stale-entity-removal",
                "Skipping stale entity soft-deletion since source already had failures.",
            )
            return

        # Everything looks good, emit the soft-deletion workunits
        for urn in last_checkpoint_state.get_urns_not_in(
            type="*", other_checkpoint_state=cur_checkpoint_state
        ):
            if urn in self._urns_to_skip:
                logger.debug(
                    f"Not soft-deleting entity {urn} since it is in urns_to_skip"
                )
                continue
            yield self._create_soft_delete_workunit(urn)

    def add_entity_to_state(self, type: str, urn: str) -> None:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(GenericCheckpointState, cur_checkpoint.state)
        cur_state.add_checkpoint_urn(type=type, urn=urn)
