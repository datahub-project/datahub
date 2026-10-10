"""Tests for QualyticsSourceConfig validation logic.

Only the validators we wrote are tested here. Pydantic's own behaviour -- defaults,
SecretStr masking, nested model parsing -- belongs to pydantic's test suite, and
DataHub treats connector tests that assert it as an automatic PR rejection.
"""

import re

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig


def _minimal(**overrides: object) -> dict[str, object]:
    return {
        "base_url": "https://acme.qualytics.io/api",
        "token": "not-a-real-token",
        **overrides,
    }


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("https://acme.qualytics.io/api/", "https://acme.qualytics.io/api"),
        ("  https://acme.qualytics.io/api  ", "https://acme.qualytics.io/api"),
        ("https://acme.qualytics.io/api///", "https://acme.qualytics.io/api"),
    ],
)
def test_base_url_trailing_slash_and_whitespace_are_stripped(
    given: str, expected: str
) -> None:
    # Paths are joined onto base_url directly, so a trailing slash would produce
    # `//datastores` and a pasted-in space would produce an unusable URL.
    assert (
        QualyticsSourceConfig.model_validate(_minimal(base_url=given)).base_url
        == expected
    )


def test_base_url_without_a_scheme_is_rejected_with_an_example() -> None:
    with pytest.raises(ValidationError) as exc:
        QualyticsSourceConfig.model_validate(_minimal(base_url="acme.qualytics.io/api"))

    message = str(exc.value)
    assert "must include a scheme" in message
    # The error has to show the fix, not just name the problem.
    assert "https://acme.qualytics.io/api" in message


def test_the_schema_accepts_the_relative_start_time_the_docs_promise() -> None:
    # Schema-driven validation (the ingestion UI, IDEs) reads the JSON schema, not the
    # runtime validator; a bare datetime field advertised date-time only.
    schema = QualyticsSourceConfig.model_json_schema()
    start_time = schema["$defs"]["QualyticsAssertionResultsConfig"]["properties"][
        "start_time"
    ]

    patterns = [s.get("pattern", "") for s in start_time.get("anyOf", [])]
    assert any(re.match(p, "-7 days") for p in patterns if p)
