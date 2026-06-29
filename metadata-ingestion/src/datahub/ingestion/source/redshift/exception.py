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
    except OSError as e:
        # redshift_connector surfaces a dropped/timed-out socket as a bare OSError
        # (e.g. "cannot read from timed out object") rather than a
        # redshift_connector.Error. Treat it as a graceful failure so a transient
        # connection drop doesn't crash the whole pipeline with an uncaught error.
        report_redshift_connection_failure(report, e)
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
    except OSError as e:
        report_redshift_connection_failure(report, e)


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
    else:
        report.failure(
            title="Failed to extract some metadata",
            message="Failed to extract some metadata from Redshift.",
            exc=e,
        )


def report_redshift_connection_failure(report: RedshiftReport, e: OSError) -> None:
    report.failure(
        title="Lost connection to Redshift",
        message=(
            "The Redshift connection dropped or timed out while reading a query result "
            "(often a slow system-table or query-history scan). Some metadata may be "
            "missing. Retrying the ingestion usually clears a transient drop. If it "
            "recurs, increase the client read timeout via the `timeout` key in "
            "`extra_client_options` (value in seconds, e.g. "
            "`extra_client_options: {timeout: 1200}`) and/or enable TCP keep-alive with "
            "`extra_client_options: {tcp_keepalive: true}` to avoid idle-connection reaps."
        ),
        exc=e,
    )
