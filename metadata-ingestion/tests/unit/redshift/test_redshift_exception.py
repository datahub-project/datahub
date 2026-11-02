import redshift_connector

from datahub.ingestion.source.redshift.exception import report_redshift_failure
from datahub.ingestion.source.redshift.report import RedshiftReport


def _programming_error(message: str) -> redshift_connector.error.ProgrammingError:
    # redshift_connector wraps the server response in a dict keyed by the
    # single-letter Postgres protocol field codes; "M" is the human-readable
    # message, matching the shape seen in real driver exceptions.
    return redshift_connector.error.ProgrammingError(
        {"S": "ERROR", "C": "XX000", "M": message}
    )


def test_permission_denied_svv_table_info_reported_as_failure() -> None:
    report = RedshiftReport()
    e = _programming_error("permission denied for relation svv_table_info")

    report_redshift_failure(report, e)

    assert len(report.failures) == 1
    assert len(report.warnings) == 0
    assert report.failures[0].title == "Permission denied"


def test_could_not_open_relation_with_oid_reported_as_warning() -> None:
    """A concurrent catalog change (e.g. an ETL job dropping/recreating a table
    mid-extraction) surfaces as 'could not open relation with OID <n>'. This is
    a transient Redshift/Postgres catalog-race condition, not a hard failure
    that should be surfaced the same way as a real permissions or connectivity
    problem, so it must land in warnings, not failures."""
    report = RedshiftReport()
    e = _programming_error("could not open relation with OID 12345")

    report_redshift_failure(report, e)

    assert len(report.warnings) == 1
    assert len(report.failures) == 0
    assert report.warnings[0].title == "Transient catalog error"


def test_could_not_open_relation_with_oid_matched_case_insensitively() -> None:
    report = RedshiftReport()
    e = _programming_error("Could Not Open Relation With OID 999")

    report_redshift_failure(report, e)

    assert len(report.warnings) == 1
    assert len(report.failures) == 0


def test_generic_redshift_error_reported_as_failure() -> None:
    report = RedshiftReport()
    e = _programming_error("connection to server was lost")

    report_redshift_failure(report, e)

    assert len(report.failures) == 1
    assert len(report.warnings) == 0
    assert report.failures[0].title == "Failed to extract some metadata"
