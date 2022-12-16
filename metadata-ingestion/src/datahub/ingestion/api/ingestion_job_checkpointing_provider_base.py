from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, NewType, Type, TypeVar

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.committable import CommitPolicy, StatefulCommittable
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

JobId = NewType("JobId", str)
CheckpointJobStateType = DatahubIngestionCheckpointClass
CheckpointJobStatesMap = Dict[JobId, CheckpointJobStateType]


@dataclass
class JobStateKey:
    pipeline_name: str
    platform_instance_id: str
    job_names: List[JobId]


class IngestionCheckpointingProviderConfig(ConfigModel):
    pass


_Self = TypeVar("_Self", bound="IngestionCheckpointingProviderBase")


@dataclass()
class IngestionCheckpointingProviderBase(
    StatefulCommittable[JobStateKey, CheckpointJobStatesMap]
):
    """
    The base class for all checkpointing state provider implementations.
    """

    def __init__(
        self, name: str, commit_policy: CommitPolicy = CommitPolicy.ON_NO_ERRORS
    ):
        # Set the initial state to an empty dict.
        super().__init__(name, commit_policy, {})

    @classmethod
    def create(
        cls: Type[_Self], config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "_Self":
        raise NotImplementedError("Sub-classes must override this method.")

    @abstractmethod
    def get_last_state(
        self,
        state_key: JobStateKey,
    ) -> CheckpointJobStatesMap:
        ...

    @abstractmethod
    def commit(self) -> None:
        ...

    @staticmethod
    def get_data_job_urn(
        orchestrator: str,
        pipeline_name: str,
        job_name: JobId,
        platform_instance_id: str,
    ) -> str:
        """
        Standardizes datajob urn minting for all ingestion job state providers.
        """
        return builder.make_data_job_urn(
            orchestrator, f"{pipeline_name}_{platform_instance_id}", job_name
        )
