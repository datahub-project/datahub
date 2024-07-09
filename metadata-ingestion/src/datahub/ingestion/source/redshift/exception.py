from typing import Any, Callable, Iterable, TypeVar, Union

import redshift_connector

from datahub.ingestion.source.redshift.report import RedshiftReport

T = TypeVar("T")


def handle_redshift_exceptions(
    report: RedshiftReport, func: Callable[..., T], *args: Any, **kwargs: Any
) -> Union[T, None]:
    try:
        return func(*args, **kwargs)
    except redshift_connector.Error as e:
        report_redshift_failure(report, e)
        return None


def handle_redshift_exceptions_yield(
    report: RedshiftReport, func: Callable[..., Iterable[T]], *args: Any, **kwargs: Any
) -> Iterable[T]:
    try:
        yield from func(*args, **kwargs)
    except redshift_connector.Error as e:
        report_redshift_failure(report, e)


def report_redshift_failure(
    report: RedshiftReport, e: redshift_connector.Error
) -> None:
    error_message = str(e).lower()
    if "permission denied" in error_message:
        if "svv_table_info" in error_message:
            report.report_failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permission to access 'svv_table_info' table. Please ensure the provided database user has access.",
                exc=e,
            )
        elif "svl_user_info" in error_message:
            report.report_failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permission to access 'svl_user_info' table. Please ensure the provided database user has access.",
                exc=e,
            )
        else:
            report.report_failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permissions.",
                exc=e,
            )
    else:
        report.report_failure(
            title="Failed to extract some metadata",
            message="Failed to extract some metadata from Redshift.",
            exc=e,
        )
