from abc import ABC, abstractmethod
from typing import Any, Dict, NewType, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

JobId = NewType("JobId", str)


class IngestionStateProvider(ABC):
    """Abstract base class for all ingestion state providers."""

    @classmethod
    @abstractmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "IngestionStateProvider":
        pass

    def get_latest_checkpoint(
        self,
        pipeline_name: str,
        platform_instance_id: str,
        job_name: JobId,
    ) -> Optional[DatahubIngestionCheckpointClass]:
        pass

    def commit_checkpoints(
        self, job_checkpoints: Dict[JobId, DatahubIngestionCheckpointClass]
    ) -> None:
        pass
