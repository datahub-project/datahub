import logging
from collections import defaultdict
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict

logger: logging.Logger = logging.getLogger(__name__)


METADATA_EXTRACTION = "Metadata Extraction"
LINEAGE_EXTRACTION = "Lineage Extraction"
USAGE_EXTRACTION_INGESTION = "Usage Extraction Ingestion"
USAGE_EXTRACTION_OPERATIONAL_STATS = "Usage Extraction Operational Stats"
USAGE_EXTRACTION_USAGE_AGGREGATION = "Usage Extraction Usage Aggregation"
EXTERNAL_TABLE_DDL_LINEAGE = "External table DDL Lineage"
VIEW_PARSING = "View Parsing"
QUERIES_EXTRACTION = "Queries Extraction"
PROFILING = "Profiling"


class IngestionHighStage(Enum):
    """
    The high-level stages at the framework level
    Team to add more stages as needed
    """

    PROFILING = "Profiling"
    _UNDEFINED = "Ingestion"


@dataclass
class IngestionStageReport:
    ingestion_high_stage_seconds: dict[IngestionHighStage, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    ingestion_stage_durations: TopKDict[str, float] = field(default_factory=TopKDict)

    def new_stage(
        self, stage: str, high_stage: IngestionHighStage = IngestionHighStage._UNDEFINED
    ) -> "IngestionStageContext":
        return IngestionStageContext(stage, self, high_stage)

    def new_high_stage(self, stage: IngestionHighStage) -> "IngestionStageContext":
        return IngestionStageContext("", self, stage)


@dataclass
class IngestionStageContext(AbstractContextManager):
    def __init__(
        self,
        stage: str,
        report: IngestionStageReport,
        high_stage: IngestionHighStage = IngestionHighStage._UNDEFINED,
    ):
        self._high_stage = high_stage
        self._ingestion_stage = (
            f"{stage} at {datetime.now(timezone.utc)}" if stage else ""
        )
        self._timer: PerfTimer = PerfTimer()
        self._report = report

    def __enter__(self) -> "IngestionStageContext":
        if self._ingestion_stage:
            logger.info(f"Stage started: {self._ingestion_stage}")
        else:
            logger.info(f"High stage started: {self._high_stage.value}")
        self._timer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = self._timer.elapsed_seconds(digits=2)
        if self._ingestion_stage:
            logger.info(
                f"Time spent in stage <{self._ingestion_stage}>: {elapsed} seconds",
                stacklevel=2,
            )
            # Store tuple as string to avoid serialization errors
            key = f"({self._high_stage.value}, {self._ingestion_stage})"
            self._report.ingestion_stage_durations[key] = elapsed
        else:
            logger.info(
                f"Time spent in stage <{self._high_stage.value}>: {elapsed} seconds",
                stacklevel=2,
            )
        self._report.ingestion_high_stage_seconds[self._high_stage] += elapsed
