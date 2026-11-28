import contextlib
import logging
from typing import Iterator

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_SQLGLOT_LOGGER,
    MSTR_SQLGLOT_SUPPRESSED_PREFIXES,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport


class SqlglotWarningFilter(logging.Filter):
    def __init__(self, report: MicroStrategyReport) -> None:
        super().__init__()
        self._report = report

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for prefix in MSTR_SQLGLOT_SUPPRESSED_PREFIXES:
            if message.startswith(prefix):
                self._report.sqlglot_warnings_suppressed[prefix] += 1
                return False
        return True


@contextlib.contextmanager
def suppress_sqlglot_warnings(report: MicroStrategyReport) -> Iterator[None]:
    sqlglot_logger = logging.getLogger(MSTR_SQLGLOT_LOGGER)
    log_filter = SqlglotWarningFilter(report)
    sqlglot_logger.addFilter(log_filter)
    try:
        yield
    finally:
        sqlglot_logger.removeFilter(log_filter)
