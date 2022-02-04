from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, List, NewType, Optional, TypeVar

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.committable import CommitPolicy, StatefulCommittable
from datahub.ingestion.api.common import PipelineContext

JobId = NewType("JobId", str)
JobState = TypeVar("JobState")
JobStatesMap = Dict[JobId, JobState]
# TODO: We need a first-class representation of a search filter in the python code. Using str for now.
JobStateFilterType = NewType("JobStateFilterType", str)


@dataclass
class JobStateKey:
    pipeline_name: str
    platform_instance_id: str
    job_names: List[JobId]


class IngestionJobStateProviderConfig(ConfigModel):
    pass


class IngestionJobStateProvider(
    StatefulCommittable[JobStateKey, JobStatesMap, JobStateFilterType],
    Generic[JobState],
):
    """
    Abstract base class for all ingestion state providers.
    This introduces the notion of ingestion pipelines and jobs for committable state providers.
    """

    def __init__(self, name: str, commit_policy: CommitPolicy):
        super(IngestionJobStateProvider, self).__init__(name, commit_policy, dict())

    @classmethod
    @abstractmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "IngestionJobStateProvider":
        """Concrete sub-classes must throw an exception if this fails."""
        pass

    def get_last_state(self, state_key: JobStateKey) -> Optional[JobStatesMap]:
        previous_states = self.get_previous_states(
            state_key=state_key, last_only=True, filter_opt=None
        )
        if previous_states:
            return previous_states[0]
        return None

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
