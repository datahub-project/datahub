import logging
import threading
from typing import Optional

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report

# Partition discovery emits warnings from worker threads (strategic-date probing runs in a
# ThreadPoolExecutor here, external-table discovery runs on executor workers downstream),
# and StructuredLogs.report_log does an unlocked read-modify-write on a shared LossyDict
# (check-then-insert / append). Every discovery warning funnels through warn(), so a single
# module-level lock serialises those concurrent writes and prevents dropped or corrupted
# warnings. Warnings are rare, so the lock is effectively uncontended.
_REPORT_WRITE_LOCK = threading.Lock()


def warn(
    report: Optional[BigQueryV2Report],
    logger: logging.Logger,
    title: str,
    message: str,
    context: Optional[str] = None,
) -> None:
    # Discovery helpers may run without a report (e.g. in tests); fall back to the logger.
    if report is not None:
        with _REPORT_WRITE_LOCK:
            report.warning(title=title, message=message, context=context)
    else:
        logger.warning(f"{message}{f' ({context})' if context else ''}")
