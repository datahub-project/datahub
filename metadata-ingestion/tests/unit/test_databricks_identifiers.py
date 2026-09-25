from typing import List, Optional

import pytest

from datahub.ingestion.source.unity.identifier_helper import split_databricks_identifier


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("a", ["a"]),
        ("a.b", ["a", "b"]),
        ("a.b.c", ["a", "b", "c"]),
        ("`a.b`.c.d", ["a.b", "c", "d"]),
        ("a.`b`.c", ["a", "b", "c"]),
        ("`a.b.c`.d", ["a.b.c", "d"]),
        ('a."b".c', ["a", "b", "c"]),
        ("", [""]),
        ("a.`unbalanced", None),
    ],
)
def test_split_databricks_identifier(raw: str, expected: Optional[List[str]]) -> None:
    assert split_databricks_identifier(raw) == expected
