import logging
import os
import pathlib
from typing import Any, Dict, Optional

import deepdiff

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.utilities.sqlglot_lineage import (
    SchemaInfo,
    SchemaResolver,
    SqlParsingResult,
    sqlglot_lineage,
)

logger = logging.getLogger(__name__)

# TODO: Hook this into the standard --update-golden-files mechanism.
UPDATE_FILES = os.environ.get("UPDATE_SQLPARSER_FILES", "false").lower() == "true"


def assert_sql_result_with_resolver(
    sql: str,
    *,
    expected_file: pathlib.Path,
    schema_resolver: SchemaResolver,
    allow_table_error: bool = False,
    **kwargs: Any,
) -> None:
    # HACK: Our BigQuery source overwrites this value and doesn't undo it.
    # As such, we need to handle that here.
    BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = "_yyyymmdd"

    res = sqlglot_lineage(
        sql,
        schema_resolver=schema_resolver,
        **kwargs,
    )

    if res.debug_info.table_error:
        if allow_table_error:
            logger.info(
                f"SQL parser table error: {res.debug_info.table_error}",
                exc_info=res.debug_info.table_error,
            )
        else:
            raise res.debug_info.table_error
    if res.debug_info.column_error:
        logger.warning(
            f"SQL parser column error: {res.debug_info.column_error}",
            exc_info=res.debug_info.column_error,
        )

    txt = res.json(indent=4)
    if UPDATE_FILES:
        expected_file.write_text(txt)
        return

    if not expected_file.exists():
        expected_file.write_text(txt)
        raise AssertionError(
            f"Expected file {expected_file} does not exist. "
            "Created it with the expected output. Please verify it."
        )

    expected = SqlParsingResult.parse_raw(expected_file.read_text())

    full_diff = deepdiff.DeepDiff(
        expected.dict(),
        res.dict(),
        exclude_regex_paths=[
            r"root.column_lineage\[\d+\].logic",
        ],
    )
    assert not full_diff, full_diff


def assert_sql_result(
    sql: str,
    *,
    dialect: str,
    platform_instance: Optional[str] = None,
    expected_file: pathlib.Path,
    schemas: Optional[Dict[str, SchemaInfo]] = None,
    **kwargs: Any,
) -> None:
    schema_resolver = SchemaResolver(
        platform=dialect, platform_instance=platform_instance
    )
    if schemas:
        for urn, schema in schemas.items():
            schema_resolver.add_raw_schema_info(urn, schema)

    assert_sql_result_with_resolver(
        sql,
        expected_file=expected_file,
        schema_resolver=schema_resolver,
        **kwargs,
    )
