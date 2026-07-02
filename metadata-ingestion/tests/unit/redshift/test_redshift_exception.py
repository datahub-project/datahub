import socket

import redshift_connector

from datahub.ingestion.source.redshift.exception import (
    handle_redshift_exceptions,
    handle_redshift_exceptions_yield,
)
from datahub.ingestion.source.redshift.report import RedshiftReport


def test_handle_redshift_exceptions_yield_catches_connection_timeout():
    # redshift_connector surfaces a socket read-timeout as a bare OSError
    # ("cannot read from timed out object"), NOT as a redshift_connector.Error.
    # A transient connection drop must be reported and end the stream gracefully
    # rather than crashing the whole ingestion pipeline with an uncaught error.
    report = RedshiftReport()

    def boom():
        raise OSError("cannot read from timed out object")
        yield  # pragma: no cover - makes this a generator

    # Must not propagate.
    list(handle_redshift_exceptions_yield(report, boom))

    assert len(report.failures) == 1


def test_handle_redshift_exceptions_catches_connection_timeout():
    report = RedshiftReport()

    def boom():
        raise socket.timeout("timed out")

    # Must not propagate; returns None like the redshift_connector.Error path.
    assert handle_redshift_exceptions(report, boom) is None
    assert len(report.failures) == 1


def test_handle_redshift_exceptions_yield_still_catches_connector_error():
    # Regression guard: the existing redshift_connector.Error path keeps working.
    report = RedshiftReport()

    def boom():
        raise redshift_connector.Error("svv_table_info permission denied")
        yield  # pragma: no cover - makes this a generator

    list(handle_redshift_exceptions_yield(report, boom))

    assert len(report.failures) == 1
