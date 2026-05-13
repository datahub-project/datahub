import re
import textwrap
from enum import Enum

import pytest
import sqlglot

from datahub.sql_parsing.query_types import get_query_type_of_sql
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT,
    sqlglot_lineage,
)
from datahub.sql_parsing.sqlglot_utils import (
    PLACEHOLDER_BACKWARD_FINGERPRINT_NORMALIZATION,
    _sanitize_snowflake_ddl,
    generalize_query,
    generalize_query_fast,
    get_dialect,
    get_query_fingerprint,
    is_dialect_instance,
)


def test_update_from_select():
    assert {"returning", "this"} == _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT


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
            'UPDATE "books" SET page_count = page_count + %s, author_count = author_count + %s WHERE book_title = %s',
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            "SELECT * FROM foo WHERE date = '2021-01-01'",
            "redshift",
            "SELECT * FROM foo WHERE date = %s",
            QueryGeneralizationTestMode.BOTH,
        ),
        (
            "SELECT * FROM books WHERE category IN ('fiction', 'biography', 'fantasy')",
            "redshift",
            "SELECT * FROM books WHERE category IN (%s)",
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
            "SELECT * FROM books WHERE zip_code IN (%s)",
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
        assert generalize_query_fast(
            query, dialect=dialect, change_table_names=True
        ) == re.sub(PLACEHOLDER_BACKWARD_FINGERPRINT_NORMALIZATION, "?", expected)


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


def test_query_fingerprint_with_secondary_id():
    query = "SELECT * FROM users WHERE id = 123"

    fingerprint1 = get_query_fingerprint(query, "snowflake")

    fingerprint2 = get_query_fingerprint(
        query, "snowflake", secondary_id="project_id_123"
    )

    fingerprint3 = get_query_fingerprint(
        query, "snowflake", secondary_id="project_id_456"
    )

    assert fingerprint1 and fingerprint2 and fingerprint3, (
        "Fingerprint should not be None"
    )
    assert fingerprint1 != fingerprint2, "Fingerprint should change with secondary_id"
    assert fingerprint2 != fingerprint3, (
        "Different secondary_id should yield different fingerprints"
    )

    fingerprint4 = get_query_fingerprint(
        query, "snowflake", secondary_id="project_id_456"
    )

    assert fingerprint3 == fingerprint4, (
        "Fingerprints are deterministic for the same secondary_id"
    )


@pytest.mark.parametrize(
    "sql, expected",
    [
        # --- Governance syntax that must be stripped ---
        # WITH TAG in column definition list (CREATE TABLE / DYNAMIC TABLE)
        (
            "CREATE TABLE db.s.t (id INT, col VARCHAR WITH TAG (schema.tag='pii'))",
            "CREATE TABLE db.s.t (id INT, col VARCHAR)",
        ),
        # WITH TAG in SELECT body (CREATE VIEW) — breaks sqlglot without this fix
        (
            "CREATE VIEW db.s.v AS SELECT id WITH TAG (schema.tag='pii'), name FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id, name FROM db.s.src",
        ),
        # Three-part tag reference
        (
            "CREATE VIEW db.s.v AS SELECT col WITH TAG (MGMT.GOV.PII_TAG='mask') FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT col FROM db.s.src",
        ),
        # WITH MASKING POLICY without USING
        (
            "CREATE VIEW db.s.v AS SELECT id WITH MASKING POLICY db.s.mp, name FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id, name FROM db.s.src",
        ),
        # WITH MASKING POLICY with USING clause
        (
            "CREATE VIEW db.s.v AS SELECT id WITH MASKING POLICY db.s.mp USING (id, name), name FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id, name FROM db.s.src",
        ),
        # WITH PROJECTION POLICY
        (
            "CREATE VIEW db.s.v AS SELECT id WITH PROJECTION POLICY db.s.pp, name FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id, name FROM db.s.src",
        ),
        # WITH ROW ACCESS POLICY — table-level
        (
            "CREATE TABLE db.s.t (id INT, name VARCHAR) WITH ROW ACCESS POLICY db.s.rap ON (id)",
            "CREATE TABLE db.s.t (id INT, name VARCHAR)",
        ),
        # WITH ROW ACCESS POLICY — multi-column ON list
        (
            "CREATE VIEW db.s.v WITH ROW ACCESS POLICY db.s.rap ON (id, region) AS SELECT id, region FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id, region FROM db.s.src",
        ),
        # Multiple governance constructs in one statement
        (
            "CREATE VIEW db.s.v AS SELECT col1 WITH TAG (s.t='v'), col2 WITH MASKING POLICY db.s.mp FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT col1, col2 FROM db.s.src",
        ),
        # Case-insensitive matching
        (
            "CREATE VIEW db.s.v AS SELECT id with tag (s.t='v') FROM db.s.src",
            "CREATE VIEW db.s.v AS SELECT id FROM db.s.src",
        ),
        # --- SQL that must NOT be modified (false-positive checks) ---
        # Plain DML
        (
            "SELECT id, name FROM db.s.src WHERE id > 0",
            "SELECT id, name FROM db.s.src WHERE id > 0",
        ),
        # CTE named TAG — WITH TAG AS (...) is a CTE, not a governance clause
        (
            "WITH TAG AS (SELECT id FROM t) SELECT * FROM TAG",
            "WITH TAG AS (SELECT id FROM t) SELECT * FROM TAG",
        ),
        # JOIN USING clause must not be touched
        (
            "SELECT a.id FROM a JOIN b USING (id)",
            "SELECT a.id FROM a JOIN b USING (id)",
        ),
        # String literal containing governance keywords must not be touched
        (
            "SELECT id, 'WITH TAG (test)' AS note FROM t",
            "SELECT id, 'WITH TAG (test)' AS note FROM t",
        ),
    ],
)
def test_sanitize_snowflake_ddl(sql: str, expected: str) -> None:
    assert _sanitize_snowflake_ddl(sql) == expected


@pytest.mark.parametrize(
    "sql, expected_in_tables",
    [
        # WITH TAG in SELECT body — parse failure without sanitization
        (
            "CREATE VIEW db.s.v AS SELECT id WITH TAG (schema.tag='pii'), name FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
        # WITH MASKING POLICY in SELECT body
        (
            "CREATE VIEW db.s.v AS SELECT id WITH MASKING POLICY db.s.mp, name FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
        # WITH PROJECTION POLICY in SELECT body
        (
            "CREATE VIEW db.s.v AS SELECT id WITH PROJECTION POLICY db.s.pp FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
        # WITH ROW ACCESS POLICY at table level
        (
            "CREATE VIEW db.s.v WITH ROW ACCESS POLICY db.s.rap ON (id) AS SELECT id FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
        # Dynamic table with column-list WITH TAG — lineage must still be extracted
        (
            "CREATE OR REPLACE DYNAMIC TABLE db.s.t (id, col WITH TAG (MGMT.GOV.TAG='mask')) "
            "target_lag='1 hour' warehouse=wh AS SELECT id, col FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
        # Multiple governance constructs on one statement
        (
            "CREATE VIEW db.s.v WITH ROW ACCESS POLICY db.s.rap ON (id) AS "
            "SELECT id WITH MASKING POLICY db.s.mp, name WITH TAG (t='v') FROM db.s.src",
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"],
        ),
    ],
)
def test_snowflake_governance_ddl_lineage(sql: str, expected_in_tables: list) -> None:
    """Snowflake governance DDL must not prevent lineage extraction."""
    resolver = SchemaResolver(platform="snowflake")
    result = sqlglot_lineage(
        sql,
        schema_resolver=resolver,
        default_db="db",
        default_schema="s",
        override_dialect="snowflake",
    )
    assert result.debug_info.table_error is None, result.debug_info.table_error
    assert sorted(result.in_tables) == sorted(expected_in_tables)
