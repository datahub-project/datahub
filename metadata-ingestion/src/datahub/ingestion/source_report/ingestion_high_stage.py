import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from enum import Enum

from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


class IngestionHighStage(Enum):
    """
    The high-level stages at the framework level
    Team to add more stages as needed
    """

    PROFILING = "Profiling"


@dataclass
class IngestionHighStageReport:
    ingestion_high_stage_seconds: dict[str, float] = field(default_factory=dict)

    def new_high_stage(self, stage: IngestionHighStage) -> "IngestionHighStageContext":
        return IngestionHighStageContext(stage, self)


@dataclass
class IngestionHighStageContext(AbstractContextManager):
    """
    The difference between this and IngestionStageContext is that this is used for high-level stages
    at the framework level not for each database or schema etc.
    """

    def __init__(self, stage: IngestionHighStage, report: IngestionHighStageReport):
        self._ingestion_high_stage = stage
        self._timer: PerfTimer = PerfTimer()
        self._report = report

    def __enter__(self) -> "IngestionHighStageContext":
        logger.info(f"High stage started: {self._ingestion_high_stage.value}")
        self._timer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = self._timer.elapsed_seconds(digits=2)
        logger.info(
            f"Time spent in stage <{self._ingestion_high_stage.value}>: {elapsed} seconds",
            stacklevel=2,
        )
        self._report.ingestion_high_stage_seconds[self._ingestion_high_stage.value] = (
            elapsed
        )
