from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.ingestion.api.committable import CommitPolicy
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_state_provider import (
    IngestionJobStateProvider,
    IngestionJobStateProviderConfig,
    JobId,
    JobStateFilterType,
    JobStateKey,
    JobStatesMap,
)
from datahub.metadata.schema_classes import DatahubIngestionRunSummaryClass

#
# Common type exports
#
JobId = JobId
JobStateKey = JobStateKey
JobStateFilterType = JobStateFilterType

#
# Reporting state specific types
#
ReportingJobStateType = DatahubIngestionRunSummaryClass
ReportingJobStatesMap = JobStatesMap[ReportingJobStateType]


class IngestionReportingProviderConfig(IngestionJobStateProviderConfig):
    pass


@dataclass()
class IngestionReportingProviderBase(IngestionJobStateProvider[ReportingJobStateType]):
    """
    The base class(non-abstract) for all reporting state provider implementations.
    This class is implemented this way as a concrete class is needed to work with the registry,
    but we don't want to implement any of the functionality yet.
    """

    def __init__(self, name: str, commit_policy: CommitPolicy = CommitPolicy.ALWAYS):
        super(IngestionReportingProviderBase, self).__init__(name, commit_policy)

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "IngestionJobStateProvider":
        raise NotImplementedError("Sub-classes must override this method.")

    def get_previous_states(
        self,
        state_key: JobStateKey,
        last_only: bool = True,
        filter_opt: Optional[JobStateFilterType] = None,
    ) -> List[ReportingJobStatesMap]:
        raise NotImplementedError("Sub-classes must override this method.")

    def commit(self) -> None:
        raise NotImplementedError("Sub-classes must override this method.")
