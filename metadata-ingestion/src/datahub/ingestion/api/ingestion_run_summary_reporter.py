from abc import ABC, abstractmethod
from typing import Any, Dict

from datahub.ingestion.api.common import PipelineContext


class IngestionRunSummaryReporter(ABC):
    @abstractmethod
    def notify_on_pipeline_start(self, ctx: PipelineContext) -> None:
        # Perform
        pass

    @abstractmethod
    def notify_on_pipeline_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        pass

    @classmethod
    @abstractmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "IngestionRunSummaryReporter":
        # Creation and initialization.
        pass
