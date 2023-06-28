import os
import pathlib
from typing import Any, Dict, Optional

import deepdiff

from datahub.utilities.sqlglot_lineage import (
    SchemaInfo,
    SchemaResolver,
    SqlParsingResult,
    sqlglot_lineage as sqlglot_tester,
)

# TODO: Hook this into the standard --update-golden-files mechanism.
UPDATE_FILES = os.environ.get("UPDATE_SQLPARSER_FILES", "false").lower() == "true"


def _assert_sql_result_with_resolver(
    sql: str,
    *,
    dialect: str,
    expected_file: pathlib.Path,
    schema_resolver: SchemaResolver,
    **kwargs: Any,
) -> None:
    res = sqlglot_tester(
        sql,
        platform=dialect,
        schema_resolver=schema_resolver,
        **kwargs,
    )

    if UPDATE_FILES:
        txt = res.json(indent=4)
        expected_file.write_text(txt)
        return

    expected = SqlParsingResult.parse_raw(expected_file.read_text())

    full_diff = deepdiff.DeepDiff(
        expected.dict(),
        res.dict(),
        exclude_regex_paths=[
            r"root.column_lineage\[\d+\].logic",
        ],
    )
    assert not full_diff, full_diff


def _assert_sql_result(
    sql: str,
    *,
    dialect: str,
    expected_file: pathlib.Path,
    schemas: Optional[Dict[str, SchemaInfo]] = None,
    **kwargs: Any,
) -> None:
    schema_resolver = SchemaResolver(platform=dialect)
    if schemas:
        for urn, schema in schemas.items():
            schema_resolver.add_raw_schema_info(urn, schema)

    _assert_sql_result_with_resolver(
        sql,
        dialect=dialect,
        expected_file=expected_file,
        schema_resolver=schema_resolver,
        **kwargs,
    )
