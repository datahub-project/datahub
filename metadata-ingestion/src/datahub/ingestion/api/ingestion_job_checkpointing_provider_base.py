# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, NewType, Optional

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

    def __init__(self, name: str, commit_policy: CommitPolicy = CommitPolicy.ALWAYS):
        # Set the initial state to an empty dict.
        super().__init__(name, commit_policy, {})

    @classmethod
    @abstractmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> Self:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @abstractmethod
    def get_latest_checkpoint(
        self,
        pipeline_name: str,
        job_name: JobId,
    ) -> Optional[DatahubIngestionCheckpointClass]:
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
