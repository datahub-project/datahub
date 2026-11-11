import re
from typing import List

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

_FIELD = DremioSQLQueries.SCHEMA_FILTER_FIELD


def _dremio_matches(patterns: List[str], schema: str, allow: bool = True) -> bool:
    """What Dremio does with the pushed condition: REGEXP_LIKE is Java's
    Matcher.find() against the UPPER()-ed schema path. Python's re.search with
    re.ASCII stands in for it: Java's `(?i)` folds ASCII letters only, and its
    `\w`/`\d`/`\s` are ASCII. This only holds for syntax both engines share
    (not possessive quantifiers or `(?P<name>...)`)."""
    condition = DremioSQLQueries.pattern_condition(patterns, _FIELD, allow=allow)
    operator = "REGEXP_LIKE" if allow else "NOT REGEXP_LIKE"
    match = re.fullmatch(
        rf"AND {operator}\({re.escape(_FIELD)}, '(.*)'\)", condition, re.DOTALL
    )
    assert match
    literal = match.group(1)
    # A lone quote would end the SQL literal early; '' is one quote.
    assert "'" not in literal.replace("''", "")
    regex = literal.replace("''", "'")
    found = re.search(regex, schema.upper(), re.ASCII) is not None
    return found if allow else not found


@pytest.mark.parametrize(
    "pattern, schema",
    [
        # Escapes keep their meaning (uppercasing them inverted \d, \w, \s).
        (r"^raw_\d+$", "raw_2024"),
        (r"^raw_\d+$", "raw_abc"),
        (r"^\w+\.staging$", "my_space.staging"),
        (r"^my_space\s?$", "my_space "),
        (r"^my_space\s?$", "my_spaceX"),
        # Case-insensitive, like AllowDenyPattern's default.
        (r"^My_Space(\..*)?$", "my_space.folder"),
        (r"^my_space\.folder$", "my_space_folder"),
        # Anchored at the start like re.match, not anywhere like find().
        ("temp", "prod.temp_x"),
        ("temp", "temp_x"),
        # A top-level | inside one pattern is anchored on both sides.
        ("sales|finance", "prod.finance"),
        ("sales|finance", "finance.x"),
        # Each pattern keeps its own group numbers for backreferences.
        (r"^(a)\1$", "aa"),
        # A quote doesn't break the SQL literal.
        ("o'brien", "o'brien.sales"),
        # Non-ASCII letters match in either case, as AllowDenyPattern does.
        ("café", "Café.sales"),
        (r"^\d+_café$", "12_CAFÉ"),
    ],
)
def test_pushed_schema_pattern_matches_like_allow_deny_pattern(
    pattern: str, schema: str
) -> None:
    expected = AllowDenyPattern(allow=[pattern])
    assert _dremio_matches([pattern], schema) is expected.allowed(schema)
    # A deny pattern drops exactly what AllowDenyPattern denies.
    denied = not AllowDenyPattern(deny=[pattern]).allowed(schema)
    assert _dremio_matches([pattern], schema, allow=False) is not denied


@pytest.mark.parametrize("allow", [True, False])
def test_multiple_patterns_each_anchored(allow: bool) -> None:
    patterns = ["raw", "staging"]
    assert _dremio_matches(patterns, "staging.orders", allow=allow) is allow
    assert _dremio_matches(patterns, "prod.staging", allow=allow) is not allow


def test_deny_everything_is_pushed() -> None:
    assert not _dremio_matches([".*"], "my_space.staging", allow=False)
    assert DremioSQLQueries.pattern_condition([".*"], _FIELD) == ""
    assert DremioSQLQueries.pattern_condition([".*", "x"], _FIELD) == ""


def test_empty_list_pushes_no_filter() -> None:
    # An empty deny must not become NOT REGEXP_LIKE(f, '(?i)^(?:)'), which
    # excludes everything.
    assert DremioSQLQueries.pattern_condition([], _FIELD, allow=False) == ""
