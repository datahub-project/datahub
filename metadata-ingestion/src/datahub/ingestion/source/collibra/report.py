import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


# Placeholder report so the client can surface skips/counters standalone. In
# source.py this becomes CollibraSourceReport(StaleEntityRemovalSourceReport); the
# warning/failure signatures below mirror the framework's structured reporting
# (message + context + exc), so swapping the base class needs no call-site changes.
@dataclass
class CollibraSourceReport:
    entities_extracted: int = 0
    pages_fetched: int = 0
    output_jobs: int = 0
    partitions_failed: int = 0
    filtered: int = 0
    warnings_list: List[str] = field(default_factory=list)
    failures_list: List[str] = field(default_factory=list)

    @staticmethod
    def _format(
        message: str, context: Optional[str], exc: Optional[BaseException]
    ) -> str:
        entry = message if context is None else f"{message} ({context})"
        return entry if exc is None else f"{entry}: {exc}"

    def warning(
        self,
        message: str,
        context: Optional[str] = None,
        title: Optional[str] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        entry = self._format(message, context, exc)
        self.warnings_list.append(entry)
        logger.warning(entry)

    def failure(
        self,
        message: str,
        context: Optional[str] = None,
        title: Optional[str] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        entry = self._format(message, context, exc)
        self.failures_list.append(entry)
        logger.error(entry)
