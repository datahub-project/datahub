import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict

logger: logging.Logger = logging.getLogger(__name__)


METADATA_EXTRACTION = "Metadata Extraction"
LINEAGE_EXTRACTION = "Lineage Extraction"
USAGE_EXTRACTION_INGESTION = "Usage Extraction Ingestion"
USAGE_EXTRACTION_OPERATIONAL_STATS = "Usage Extraction Operational Stats"
USAGE_EXTRACTION_USAGE_AGGREGATION = "Usage Extraction Usage Aggregation"
PROFILING = "Profiling"


@dataclass
class IngestionStageReport:
    ingestion_stage: Optional[str] = None
    ingestion_stage_durations: TopKDict[str, float] = field(default_factory=TopKDict)

    _timer: Optional[PerfTimer] = field(
        default=None, init=False, repr=False, compare=False
    )

    def report_ingestion_stage_start(self, stage: str) -> None:
        if self._timer:
            elapsed = round(self._timer.elapsed_seconds(), 2)
            logger.info(
                f"Time spent in stage <{self.ingestion_stage}>: {elapsed} seconds"
            )
            if self.ingestion_stage:
                self.ingestion_stage_durations[self.ingestion_stage] = elapsed
        else:
            self._timer = PerfTimer()

        self.ingestion_stage = f"{stage} at {datetime.now(timezone.utc)}"
        self._timer.start()
