import pytest

from datahub.ingestion.source.qlik_sense.report_text import describe_error


@pytest.mark.parametrize(
    "error, text",
    [
        (ValueError("bad value"), "ValueError: bad value"),
        (RuntimeError(), "RuntimeError"),
    ],
)
def test_describe_error_names_the_type(error: Exception, text: str) -> None:
    assert describe_error(error) == text
