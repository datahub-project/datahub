import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


# Placeholder report so the client can surface skips/counters standalone. In
# source.py this becomes CollibraSourceReport(StaleEntityRemovalSourceReport) and
# `warning` delegates to the framework's structured reporting.
@dataclass
class CollibraSourceReport:
    entities_extracted: int = 0
    pages_fetched: int = 0
    output_jobs: int = 0
    partitions_failed: int = 0
    filtered: int = 0
    warnings_list: List[str] = field(default_factory=list)

    def warning(self, message: str) -> None:
        self.warnings_list.append(message)
        logger.warning(message)
