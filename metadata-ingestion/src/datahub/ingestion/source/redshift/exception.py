from typing import Callable, Iterable, TypeVar, Union

import redshift_connector
from typing_extensions import ParamSpec

from datahub.ingestion.source.redshift.report import RedshiftReport

T = TypeVar("T")
P = ParamSpec("P")


def handle_redshift_exceptions(
    report: RedshiftReport,
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Union[T, None]:
    try:
        return func(*args, **kwargs)
    except redshift_connector.Error as e:
        report_redshift_failure(report, e)
        return None


def handle_redshift_exceptions_yield(
    report: RedshiftReport,
    func: Callable[P, Iterable[T]],
    *args: P.args,
    **kwargs: P.kwargs,
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
            report.failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permission to access 'svv_table_info' table. Please ensure the provided database user has access.",
                exc=e,
            )
        elif "svl_user_info" in error_message:
            report.failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permission to access 'svl_user_info' table. Please ensure the provided database user has access.",
                exc=e,
            )
        else:
            report.failure(
                title="Permission denied",
                message="Failed to extract metadata due to insufficient permissions.",
                exc=e,
            )
    elif "could not open relation with oid" in error_message:
        report.warning(
            title="Transient catalog error",
            message="Failed to extract some metadata because a referenced relation was concurrently dropped or recreated on the Redshift cluster (e.g. by a CTAS-and-swap ETL job). This is usually a transient catalog-race condition and may resolve on the next scheduled run.",
            exc=e,
        )
    else:
        report.failure(
            title="Failed to extract some metadata",
            message="Failed to extract some metadata from Redshift.",
            exc=e,
        )
