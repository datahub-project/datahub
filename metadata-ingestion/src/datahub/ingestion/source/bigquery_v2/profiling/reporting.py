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
    # Discovery helpers may run without a report (e.g. in tests); fall back to the logger.
    if report is not None:
        report.warning(title=title, message=message, context=context)
    else:
        logger.warning(f"{message}{f' ({context})' if context else ''}")
