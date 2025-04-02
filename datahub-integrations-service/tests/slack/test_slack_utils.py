from datahub.testing.doctest import assert_doctest

from datahub_integrations.slack.utils import numbers, time


def test_time_utils() -> None:
    assert_doctest(time)


def test_numbers_utils() -> None:
    assert_doctest(numbers)
