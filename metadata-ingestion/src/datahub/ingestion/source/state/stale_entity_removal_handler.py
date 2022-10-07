import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Generic, Iterable, List, Optional, Tuple, Type, TypeVar, cast

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.ingestion_job_state_provider import JobId
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.metadata.schema_classes import ChangeTypeClass, StatusClass

logger: logging.Logger = logging.getLogger(__name__)


class StatefulStaleMetadataRemovalConfig(StatefulIngestionConfig):
    """
    Base specialized config for Stateful Ingestion with stale metadata removal capability.
    """

    _entity_types: List[str] = []
    remove_stale_metadata: bool = pydantic.Field(
        default=True,
        description=f"Soft-deletes the entities of type {', '.join(_entity_types)} in the last successful run but missing in the current run with stateful_ingestion enabled.",
    )
    fail_safe_threshold: float = pydantic.Field(
        default=20.0,
        description="Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
        le=100.0,  # mypy does not work with pydantic.confloat. This is the recommended work-around.
        ge=0.0,
    )


@dataclass
class StaleEntityRemovalSourceReport(StatefulIngestionReport):
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)


Derived = TypeVar("Derived", bound=CheckpointStateBase)


class StaleEntityCheckpointStateBase(CheckpointStateBase, ABC, Generic[Derived]):
    """
    Defines the abstract interface for the checkpoint states that are used for stale entity removal.
    Examples include sql_common state for tracking table and & view urns,
    dbt that tracks node & assertion urns, kafka state tracking topic urns.
    """

    @classmethod
    @abstractmethod
    def get_supported_types(cls) -> List[str]:
        pass

    @abstractmethod
    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        """
        Adds an urn into the list used for tracking the type.
        :param type: The type of the urn such as a 'table', 'view',
         'node', 'topic', 'assertion' that the concrete sub-class understands.
        :param urn: The urn string
        :return: None.
        """
        pass

    @abstractmethod
    def get_urns_not_in(
        self, type: str, other_checkpoint_state: Derived
    ) -> Iterable[str]:
        """
        Gets the urns present in this checkpoint but not the other_checkpoint for the given type.
        :param type: The type of the urn such as a 'table', 'view',
         'node', 'topic', 'assertion' that the concrete sub-class understands.
        :param other_checkpoint_state: the checkpoint state to compute the urn set difference against.
        :return: an iterable to the set of urns present in this checkpoing state but not in the other_checkpoint.
        """
        pass

    @abstractmethod
    def get_percent_entities_changed(self, old_checkpoint_state: Derived) -> float:
        """
        Returns the percentage of entities that have changed relative to `old_checkpoint_state`.
        :param old_checkpoint_state: the old checkpoint state to compute the relative change percent against.
        :return: (1-|intersection(self, old_checkpoint_state)| / |old_checkpoint_state|) * 100.0
        """
        pass

    @staticmethod
    def compute_percent_entities_changed(
        new_old_entity_list: List[Tuple[List[str], List[str]]]
    ) -> float:
        old_count_all = 0
        overlap_count_all = 0
        for new_entities, old_entities in new_old_entity_list:
            (
                overlap_count,
                old_count,
                _,
            ) = StaleEntityCheckpointStateBase.get_entity_overlap_and_cardinalities(
                new_entities=new_entities, old_entities=old_entities
            )
            overlap_count_all += overlap_count
            old_count_all += old_count
        if old_count_all:
            return (1 - overlap_count / old_count_all) * 100.0
        return 0.0

    @staticmethod
    def get_entity_overlap_and_cardinalities(
        new_entities: List[str], old_entities: List[str]
    ) -> Tuple[int, int, int]:
        new_set = set(new_entities)
        old_set = set(old_entities)
        return len(new_set.intersection(old_set)), len(old_set), len(new_set)


class StaleEntityRemovalHandler(
    StatefulIngestionUsecaseHandlerBase[StaleEntityCheckpointStateBase]
):
    """
    The stateful ingestion helper class that handles stale entity removal.
    This contains the generic logic for all sources that need to support stale entity removal for all the states
    derived from StaleEntityCheckpointStateBase. This uses the template method pattern on CRTP based derived state
    class hierarchies.
    """

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: Optional[StatefulIngestionConfigBase],
        state_type_class: Type[StaleEntityCheckpointStateBase],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.config = config
        self.source = source
        self.state_type_class = state_type_class
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.stateful_ingestion_config = (
            cast(StatefulStaleMetadataRemovalConfig, self.config.stateful_ingestion)
            if self.config
            else None
        )
        self.checkpointing_enabled: bool = (
            True
            if (
                source.is_stateful_ingestion_configured()
                and self.stateful_ingestion_config
                and self.stateful_ingestion_config.remove_stale_metadata
            )
            else False
        )
        self._job_id = self._init_job_id()
        self.source.register_stateful_ingestion_usecase_handler(self)

    def _init_job_id(self) -> JobId:
        # Handle backward-compatibility for existing sources.
        backward_comp_platform_to_job_name: Dict[str, str] = {
            "bigquery": "ingest_from_bigquery_source",
            "dbt": "dbt_stateful_ingestion",
            "glue": "glue_stateful_ingestion",
            "kafka": "ingest_from_kafka_source",
            "pulsar": "ingest_from_pulsar_source",
            "snowflake": "common_ingest_from_sql_source",
        }
        platform: Optional[str] = getattr(self.source, "platform")
        if platform in backward_comp_platform_to_job_name:
            return JobId(backward_comp_platform_to_job_name[platform])

        # Default name for everything else
        job_name_suffix = "stale_entity_removal"
        return JobId(f"{platform}_{job_name_suffix}" if platform else job_name_suffix)

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

    def is_checkpointing_enabled(self) -> bool:
        return self.checkpointing_enabled

    def create_checkpoint(self) -> Optional[Checkpoint]:
        if self.is_checkpointing_enabled() and not self._ignore_new_state():
            assert self.stateful_ingestion_config is not None
            assert self.pipeline_name is not None
            return Checkpoint(
                job_name=self.job_id,
                pipeline_name=self.pipeline_name,
                platform_instance_id=self.source.get_platform_instance_id(),
                run_id=self.run_id,
                config=cast(ConfigModel, self.config),
                state=self.state_type_class(),
            )
        return None

    def _create_soft_delete_workunit(self, urn: str, type: str) -> MetadataWorkUnit:
        entity_type = type
        if entity_type in ["view", "table", "topic"]:
            entity_type = "dataset"

        logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
        mcp = MetadataChangeProposalWrapper(
            entityType=entity_type,
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
        if not self.is_checkpointing_enabled() or self._ignore_old_state():
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

        # Check if the entity delta is below the fail-safe threshold.
        entity_difference_percent = cur_checkpoint_state.get_percent_entities_changed(
            last_checkpoint_state
        )
        assert self.stateful_ingestion_config
        if (
            entity_difference_percent
            > self.stateful_ingestion_config.fail_safe_threshold
        ):
            # Log the failure. This would prevent the current state from getting committed.
            self.source.get_report().report_failure(
                "Stateful Ingestion",
                f"Fail safe mode triggered, entity difference percent:{entity_difference_percent}"
                " > fail_safe_threshold:{self.stateful_ingestion_config.fail_safe_threshold}",
            )
            # Bail so that we don't emit the stale entity removal workunits.
            return

        # Everything looks good, emit the soft-deletion workunits
        for type in self.state_type_class.get_supported_types():
            for urn in last_checkpoint_state.get_urns_not_in(
                type=type, other_checkpoint_state=cur_checkpoint_state
            ):
                yield self._create_soft_delete_workunit(urn, type)

    def add_entity_to_state(self, type: str, urn: str) -> None:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return
        cur_checkpoint = self.source.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(StaleEntityCheckpointStateBase, cur_checkpoint.state)
        cur_state.add_checkpoint_urn(type=type, urn=urn)
