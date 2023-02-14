from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, NewType

from typing_extensions import Self

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.committable import CommitPolicy, StatefulCommittable
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

JobId = NewType("JobId", str)
CheckpointJobStateType = DatahubIngestionCheckpointClass
CheckpointJobStatesMap = Dict[JobId, CheckpointJobStateType]


class IngestionCheckpointingProviderConfig(ConfigModel):
    pass


@dataclass()
class IngestionCheckpointingProviderBase(StatefulCommittable[CheckpointJobStatesMap]):
    """
    The base class for all checkpointing state provider implementations.
    """

    def __init__(
        self, name: str, commit_policy: CommitPolicy = CommitPolicy.ON_NO_ERRORS
    ):
        # Set the initial state to an empty dict.
        super().__init__(name, commit_policy, {})

    @classmethod
    @abstractmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "Self":
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @staticmethod
    def get_data_job_urn(
        orchestrator: str,
        pipeline_name: str,
        job_name: JobId,
    ) -> str:
        """
        Standardizes datajob urn minting for all ingestion job state providers.
        """
        return builder.make_data_job_urn(orchestrator, pipeline_name, job_name)

    @staticmethod
    def get_data_job_legacy_urn(
        orchestrator: str,
        pipeline_name: str,
        job_name: JobId,
        platform_instance_id: str,
    ) -> str:
        return IngestionCheckpointingProviderBase.get_data_job_urn(
            orchestrator, f"{pipeline_name}_{platform_instance_id}", job_name
        )
