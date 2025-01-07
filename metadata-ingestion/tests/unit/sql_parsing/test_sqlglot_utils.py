import textwrap
from enum import Enum

import pytest
import sqlglot

from datahub.sql_parsing.query_types import get_query_type_of_sql
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT
from datahub.sql_parsing.sqlglot_utils import (
    generalize_query,
    generalize_query_fast,
    get_dialect,
    get_query_fingerprint,
    is_dialect_instance,
)


def test_update_from_select():
    assert _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT == {"returning", "this"}


def test_is_dialect_instance():
    snowflake = get_dialect("snowflake")

    assert is_dialect_instance(snowflake, "snowflake")
    assert not is_dialect_instance(snowflake, "bigquery")

    redshift = get_dialect("redshift")
    assert is_dialect_instance(redshift, ["redshift", "snowflake"])
    assert is_dialect_instance(redshift, ["postgres", "snowflake"])


def test_query_types():
    assert get_query_type_of_sql(
        sqlglot.parse_one(
            "create temp table foo as select * from bar", dialect="redshift"
        ),
        dialect="redshift",
    ) == (QueryType.CREATE_TABLE_AS_SELECT, {"kind": "TABLE", "temporary": True})

    assert get_query_type_of_sql(
        sqlglot.parse_one("create table #foo as select * from bar", dialect="redshift"),
        dialect="redshift",
    ) == (QueryType.CREATE_TABLE_AS_SELECT, {"kind": "TABLE", "temporary": True})

    assert get_query_type_of_sql(
        sqlglot.parse_one("create view foo as select * from bar", dialect="redshift"),
        dialect="redshift",
    ) == (QueryType.CREATE_VIEW, {"kind": "VIEW"})


class QueryGeneralizationTestMode(Enum):
    FULL = "full"
    FAST = "fast"
    BOTH = "both"


@pytest.mark.parametrize(
    "query, dialect, expected, mode",
    [
        # Basic keyword normalization.
        (
            "select * from foo",
            "redshift",
            "SELECT * FROM foo",
            QueryGeneralizationTestMode.FULL,
        ),
        # Comment removal and whitespace normalization.
        (
            "/* query system = foo, id = asdf */\nSELECT /* inline comment */ *\nFROM foo",
            "redshift",
            "SELECT * FROM foo",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            "SELECT a\n -- comment--\n,b --another comment\n FROM books",
            "redshift",
            "SELECT a, b FROM books",
            QueryGeneralizationTestMode.BOTH,
        ),
        # Parameter normalization.
        (
            "UPDATE  \"books\" SET page_count = page_count + 1, author_count = author_count + 1 WHERE book_title = 'My New Book'",
            "redshift",
            'UPDATE "books" SET page_count = page_count + ?, author_count = author_count + ? WHERE book_title = ?',
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            "SELECT * FROM foo WHERE date = '2021-01-01'",
            "redshift",
            "SELECT * FROM foo WHERE date = ?",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            "SELECT * FROM books WHERE category IN ('fiction', 'biography', 'fantasy')",
            "redshift",
            "SELECT * FROM books WHERE category IN (?)",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            textwrap.dedent(
                """\
                INSERT INTO MyTable
                (Column1, Column2, Column3)
                VALUES
                ('John', 123, 'Lloyds Office');
                """
            ),
            "mssql",
            "INSERT INTO MyTable (Column1, Column2, Column3) VALUES (?)",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            # Uneven spacing within the IN clause.
            "SELECT * FROM books WHERE zip_code IN (123,345, 423 )",
            "redshift",
            "SELECT * FROM books WHERE zip_code IN (?)",
            QueryGeneralizationTestMode.BOTH,
        ),
        # Uneven spacing in the column list.
        # This isn't perfect e.g. we still have issues with function calls inside selects.
        (
            "SELECT a\n  ,b FROM books",
            "redshift",
            "SELECT a, b FROM books",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            textwrap.dedent(
                """\
                /* Copied from https://stackoverflow.com/a/452934/5004662 */
                INSERT INTO MyTable
                (Column1, Column2, Column3)
                VALUES
                /* multiple value rows */
                ('John', 123, 'Lloyds Office'),
                ('Jane', 124, 'Lloyds Office'),
                ('Billy', 125, 'London Office'),
                ('Miranda', 126, 'Bristol Office');
                """
            ),
            "mssql",
            "INSERT INTO MyTable (Column1, Column2, Column3) VALUES (?), (?), (?), (?)",
            QueryGeneralizationTestMode.FULL,
        ),
        # Test table name normalization.
        # These are only supported with fast normalization.
        (
            "SELECT * FROM datahub_community.fivetran_interval_unconstitutional_staging.datahub_slack_mess-staging-480fd5a7-58f4-4cc9-b6fb-87358788efe6",
            "bigquery",
            "SELECT * FROM datahub_community.fivetran_interval_unconstitutional_staging.datahub_slack_mess-staging-00000000-0000-0000-0000-000000000000",
            QueryGeneralizationTestMode.FAST,
        ),
        (
            "SELECT * FROM datahub_community.maggie.commonroom_slack_members_20240315",
            "bigquery",
            "SELECT * FROM datahub_community.maggie.commonroom_slack_members_YYYYMMDD",
            QueryGeneralizationTestMode.FAST,
        ),
        (
            "SELECT COUNT(*) FROM ge_temp_aa91f1fd",
            "bigquery",
            "SELECT COUNT(*) FROM ge_temp_abcdefgh",
            QueryGeneralizationTestMode.FAST,
        ),
    ],
)
def test_query_generalization(
    query: str, dialect: str, expected: str, mode: QueryGeneralizationTestMode
) -> None:
    if mode in {QueryGeneralizationTestMode.FULL, QueryGeneralizationTestMode.BOTH}:
        assert generalize_query(query, dialect=dialect) == expected
    if mode in {QueryGeneralizationTestMode.FAST, QueryGeneralizationTestMode.BOTH}:
        assert (
            generalize_query_fast(query, dialect=dialect, change_table_names=True)
            == expected
        )


def test_query_fingerprint():
    assert get_query_fingerprint(
        "select * /* everything */ from foo where ts = 34", platform="redshift"
    ) == get_query_fingerprint("SELECT * FROM foo where ts = 38", platform="redshift")

    assert get_query_fingerprint(
        "select 1 + 1", platform="postgres"
    ) != get_query_fingerprint("select 2", platform="postgres")


def test_redshift_query_fingerprint():
    query1 = "insert into insert_into_table (select * from base_table);"
    query2 = "INSERT INTO insert_into_table (SELECT * FROM base_table)"

    assert get_query_fingerprint(query1, "redshift") == get_query_fingerprint(
        query2, "redshift"
    )
    assert get_query_fingerprint(query1, "redshift", True) != get_query_fingerprint(
        query2, "redshift", True
    )
