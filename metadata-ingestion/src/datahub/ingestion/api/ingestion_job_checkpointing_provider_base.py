from dataclasses import dataclass
from typing import Any, Dict

from datahub.ingestion.api.committable import CommitPolicy
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_state_provider import (
    IngestionJobStateProvider,
    IngestionJobStateProviderConfig,
    JobId,
    JobStateKey,
    JobStatesMap,
)
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

#
# Common type exports
#
JobId = JobId
JobStateKey = JobStateKey

#
# Checkpoint state specific types
#
CheckpointJobStateType = DatahubIngestionCheckpointClass
CheckpointJobStatesMap = JobStatesMap[CheckpointJobStateType]


class IngestionCheckpointingProviderConfig(IngestionJobStateProviderConfig):
    pass


@dataclass()
class IngestionCheckpointingProviderBase(
    IngestionJobStateProvider[CheckpointJobStateType]
):
    """
    The base class(non-abstract) for all checkpointing state provider implementations.
    This class is implemented this way as a concrete class is needed to work with the registry,
    but we don't want to implement any of the functionality yet.
    """

    def __init__(
        self, name: str, commit_policy: CommitPolicy = CommitPolicy.ON_NO_ERRORS
    ):
        super(IngestionCheckpointingProviderBase, self).__init__(name, commit_policy)

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "IngestionJobStateProvider":
        raise NotImplementedError("Sub-classes must override this method.")

    def get_last_state(
        self,
        state_key: JobStateKey,
    ) -> CheckpointJobStatesMap:
        raise NotImplementedError("Sub-classes must override this method.")

    def commit(self) -> None:
        raise NotImplementedError("Sub-classes must override this method.")
