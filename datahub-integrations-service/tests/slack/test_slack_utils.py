from datahub.testing.doctest import assert_doctest

from datahub_integrations.slack.utils import numbers, time, urls
from datahub_integrations.slack.utils.string import truncate


def test_time_utils() -> None:
    assert_doctest(time)


def test_numbers_utils() -> None:
    assert_doctest(numbers)


def test_urls_utils() -> None:
    assert_doctest(urls)


def test_string_truncate() -> None:
    assert truncate("hello", max_length=5) == "hello"
    assert truncate("hello world", max_length=8) == "hello..."
    assert (
        truncate("hello world", max_length=5, show_length=True)
        == "hello[6 chars truncated]"
    )
