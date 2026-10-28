from typing import Iterator

import pytest
import redshift_connector

from datahub.ingestion.source.redshift.exception import (
    handle_redshift_exceptions,
    handle_redshift_exceptions_yield,
    report_redshift_failure,
)
from datahub.ingestion.source.redshift.report import RedshiftReport


@pytest.mark.parametrize(
    "message, expected_title",
    [
        ("permission denied for relation svv_table_info", "Permission denied"),
        ("permission denied for relation svl_user_info", "Permission denied"),
        ("permission denied for schema pg_catalog", "Permission denied"),
        ("connection reset by peer", "Failed to extract some metadata"),
    ],
)
def test_report_redshift_failure_classifies_driver_errors(
    message: str, expected_title: str
) -> None:
    report = RedshiftReport()
    report_redshift_failure(report, redshift_connector.Error(message))
    assert [f.title for f in report.failures] == [expected_title]


def test_handle_redshift_exceptions_returns_result_or_none_on_driver_error() -> None:
    report = RedshiftReport()

    assert handle_redshift_exceptions(report, lambda x: x + 1, 1) == 2
    assert not report.failures

    def boom() -> int:
        raise redshift_connector.Error("permission denied")

    assert handle_redshift_exceptions(report, boom) is None
    assert len(report.failures) == 1


def test_handle_redshift_exceptions_yield_keeps_items_before_failure() -> None:
    report = RedshiftReport()

    def produce() -> Iterator[int]:
        yield 1
        yield 2
        raise redshift_connector.Error("permission denied for relation svv_table_info")

    assert list(handle_redshift_exceptions_yield(report, produce)) == [1, 2]
    assert len(report.failures) == 1


def test_handle_redshift_exceptions_do_not_swallow_other_errors() -> None:
    report = RedshiftReport()

    def boom() -> int:
        raise ValueError("not a driver error")

    with pytest.raises(ValueError):
        handle_redshift_exceptions(report, boom)
    with pytest.raises(ValueError):
        list(handle_redshift_exceptions_yield(report, lambda: (_ for _ in [boom()])))
    assert not report.failures
