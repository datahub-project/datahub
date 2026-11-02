import re

import pytest

from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

_FIELD = "UPPER(TABLE_SCHEMA)"


def _pushed_regex(pattern: str) -> str:
    condition = DremioSQLQueries.pattern_condition([pattern], _FIELD)
    match = re.fullmatch(rf"AND REGEXP_LIKE\({re.escape(_FIELD)}, '(.*)'\)", condition)
    assert match
    return match.group(1)


@pytest.mark.parametrize(
    "pattern, schema, expected",
    [
        (r"^raw_\d+$", "raw_2024", True),
        (r"^raw_\d+$", "raw_abc", False),
        (r"^\w+\.staging$", "my_space.staging", True),
        (r"^my_space\s?$", "my_space ", True),
        (r"^my_space\s?$", "my_spaceX", False),
        (r"^My_Space(\..*)?$", "my_space.folder", True),
        (r"^my_space\.folder$", "my_space_folder", False),
    ],
)
def test_schema_pattern_pushed_to_dremio_keeps_its_meaning(
    pattern: str, schema: str, expected: bool
) -> None:
    # Dremio matches against the UPPER()-ed schema path. Uppercasing the
    # pattern must not change escapes: \d would become \D (any non-digit).
    assert bool(re.search(_pushed_regex(pattern), schema.upper())) is expected
