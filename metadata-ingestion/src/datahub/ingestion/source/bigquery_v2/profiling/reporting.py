"""Reporting helper for the profiling subpackage.

Partition discovery helpers may run without a report attached (e.g. in unit tests),
so warnings must degrade to the module logger when no report is available. Surfacing
skips/timeouts/permission errors to the ingestion report is important: operators read
the report, not the debug logs.
"""

import logging
from typing import Optional

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report


def warn(
    report: Optional[BigQueryV2Report],
    logger: logging.Logger,
    title: str,
    message: str,
    context: Optional[str] = None,
) -> None:
    if report is not None:
        report.warning(title=title, message=message, context=context)
    else:
        logger.warning(f"{message}{f' ({context})' if context else ''}")
