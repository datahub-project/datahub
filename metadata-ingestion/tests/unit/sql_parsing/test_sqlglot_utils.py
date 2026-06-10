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
    _sanitize_tsql_temp_tables,
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


# Two ETL/sync runs of the same load differ only by the per-run ephemeral staging
# table name (a UUID). With temp-aware normalization they must collapse to one
# fingerprint (one Query entity), instead of minting a new entity every run.
_STAGING_MERGE = (
    'MERGE INTO "DB"."SCH"."{dest}" AS m '
    'USING "DB"."FIVETRAN_X_STAGING"."T-STAGING-{uuid}" AS s '
    "ON m.id = s.id WHEN MATCHED THEN UPDATE SET m.v = s.v"
)


def test_temp_aware_fingerprint_collapses_per_run_staging_uuid():
    from datahub.sql_parsing.sqlglot_utils import DEFAULT_TEMP_TABLE_FINGERPRINT_RULES

    run1 = _STAGING_MERGE.format(
        dest="DEST", uuid="e089db10-8bb1-4989-9859-ea3ec9387b50"
    )
    run2 = _STAGING_MERGE.format(
        dest="DEST", uuid="aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
    )
    fp1 = get_query_fingerprint(
        run1,
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    fp2 = get_query_fingerprint(
        run2,
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    assert fp1 == fp2


def test_temp_aware_fingerprint_keeps_distinct_real_targets():
    # Same staging-shaped source but DIFFERENT real destinations => different
    # lineage => must stay distinct fingerprints.
    from datahub.sql_parsing.sqlglot_utils import DEFAULT_TEMP_TABLE_FINGERPRINT_RULES

    uuid = "e089db10-8bb1-4989-9859-ea3ec9387b50"
    fp_a = get_query_fingerprint(
        _STAGING_MERGE.format(dest="DEST_A", uuid=uuid),
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    fp_b = get_query_fingerprint(
        _STAGING_MERGE.format(dest="DEST_B", uuid=uuid),
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    assert fp_a != fp_b


@pytest.mark.parametrize(
    "name_a,name_b",
    [
        # Segment per-sync uuid (underscore form)
        (
            '"DB"."S"."segment_aaaaaaaa_1111_2222_3333_444444444444"',
            '"DB"."S"."segment_bbbbbbbb_5555_6666_7777_888888888888"',
        ),
        # Great Expectations per-run temp tables
        ('"DB"."S"."GE_TMP_0a1b2c3d"', '"DB"."S"."GE_TMP_9f8e7d6c"'),
        # pysnowflake temp_<32hex>
        (
            '"DB"."S"."temp_0a1b2c3d4e5f60718293a4b5c6d7e8f90"',
            '"DB"."S"."temp_ffffffff00000000ffffffff00000000"',
        ),
    ],
)
def test_temp_aware_fingerprint_collapses_per_run_etl_markers(name_a, name_b):
    from datahub.sql_parsing.sqlglot_utils import DEFAULT_TEMP_TABLE_FINGERPRINT_RULES

    fp_a = get_query_fingerprint(
        f"SELECT * FROM {name_a}",
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    fp_b = get_query_fingerprint(
        f"SELECT * FROM {name_b}",
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    assert fp_a == fp_b


def test_temp_aware_fingerprint_does_not_merge_distinct_dbt_models():
    # dbt `<model>__dbt_tmp` names are STABLE per model (no per-run token), so they
    # don't cause the explosion and must NOT be collapsed — that would merge the
    # lineage of two different real models.
    from datahub.sql_parsing.sqlglot_utils import DEFAULT_TEMP_TABLE_FINGERPRINT_RULES

    fp_orders = get_query_fingerprint(
        'CREATE TABLE "DB"."S"."orders__dbt_tmp" AS SELECT 1',
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    fp_customers = get_query_fingerprint(
        'CREATE TABLE "DB"."S"."customers__dbt_tmp" AS SELECT 1',
        "snowflake",
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    assert fp_orders != fp_customers


def test_temp_table_patterns_default_none_is_unchanged():
    # Regression guard: omitting the param (or None) must be byte-identical to today.
    q = "SELECT a FROM t WHERE x = 1"
    assert get_query_fingerprint(q, "snowflake", fast=True) == get_query_fingerprint(
        q, "snowflake", fast=True, temp_table_patterns=None
    )


@pytest.mark.parametrize(
    "platform,name_a,name_b",
    [
        # Snowflake / Redshift / Postgres style: double-quoted identifiers.
        (
            "snowflake",
            '"DB"."S"."T-STAGING-aaaaaaaa-1111-2222-3333-444444444444"',
            '"DB"."S"."T-STAGING-bbbbbbbb-5555-6666-7777-888888888888"',
        ),
        # BigQuery style: backtick-quoted identifiers.
        (
            "bigquery",
            "`proj`.`ds`.`T-STAGING-aaaaaaaa-1111-2222-3333-444444444444`",
            "`proj`.`ds`.`T-STAGING-bbbbbbbb-5555-6666-7777-888888888888`",
        ),
        # MSSQL style: bracket-quoted identifiers.
        (
            "mssql",
            "[db].[s].[T-STAGING-aaaaaaaa-1111-2222-3333-444444444444]",
            "[db].[s].[T-STAGING-bbbbbbbb-5555-6666-7777-888888888888]",
        ),
    ],
)
def test_temp_aware_fingerprint_is_dialect_quoting_aware(platform, name_a, name_b):
    # The same per-run staging token must collapse regardless of the dialect's
    # identifier quoting (double-quote / backtick / bracket).
    from datahub.sql_parsing.sqlglot_utils import DEFAULT_TEMP_TABLE_FINGERPRINT_RULES

    fp_a = get_query_fingerprint(
        f"SELECT * FROM {name_a}",
        platform,
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    fp_b = get_query_fingerprint(
        f"SELECT * FROM {name_b}",
        platform,
        fast=True,
        temp_table_patterns=DEFAULT_TEMP_TABLE_FINGERPRINT_RULES,
    )
    assert fp_a == fp_b


def test_slow_path_temp_normalization_by_table_name():
    # On the slow/AST path, temp tables are normalized by matching the RESOLVED
    # table name against the connector's temporary_tables_pattern (no raw-text
    # quoting regexes). Two runs differing only by a per-run staging table
    # collapse; a different real destination stays distinct.
    name_patterns = [re.compile(r".*\.fivetran_.*_staging\..*", re.IGNORECASE)]

    def merge(dest: str, uuid: str) -> str:
        return (
            f'MERGE INTO "DB"."SCH"."{dest}" AS m '
            f'USING "DB"."FIVETRAN_X_STAGING"."T-STAGING-{uuid}" AS s '
            "ON m.id = s.id WHEN MATCHED THEN UPDATE SET m.v = s.v"
        )

    fp1 = get_query_fingerprint(
        merge("DEST", "e089db10-8bb1-4989-9859-ea3ec9387b50"),
        "snowflake",
        temp_table_patterns=name_patterns,
    )
    fp2 = get_query_fingerprint(
        merge("DEST", "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"),
        "snowflake",
        temp_table_patterns=name_patterns,
    )
    assert fp1 == fp2

    fp_other = get_query_fingerprint(
        merge("OTHER", "e089db10-8bb1-4989-9859-ea3ec9387b50"),
        "snowflake",
        temp_table_patterns=name_patterns,
    )
    assert fp_other != fp1


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


@pytest.mark.parametrize(
    "sql, expected",
    [
        # --- #<digit> temp names: must be bracketed so sqlglot stops lexing the
        # leading digits as a NUMBER (otherwise the whole statement fails to parse) ---
        (
            "SELECT a INTO #63114Actual FROM real_src",
            "SELECT a INTO [#63114Actual] FROM real_src",
        ),
        (
            "SELECT a FROM #2025CourseCompletions",
            "SELECT a FROM [#2025CourseCompletions]",
        ),
        # Global (##) temp table
        ("SELECT a FROM ##2025Global", "SELECT a FROM [##2025Global]"),
        # Bare #<digit>
        ("DROP TABLE #1", "DROP TABLE [#1]"),
        # --- Must NOT be modified ---
        # Letter-leading temp names parse fine already
        ("SELECT a FROM #temp", "SELECT a FROM #temp"),
        # Already bracketed
        ("SELECT a FROM [#63114Actual]", "SELECT a FROM [#63114Actual]"),
        # #<digit> inside a string literal is data, not a table
        ("SELECT '#123 order' AS c FROM t", "SELECT '#123 order' AS c FROM t"),
        # #<digit> inside a line comment
        ("SELECT a FROM t -- ticket #123\n", "SELECT a FROM t -- ticket #123\n"),
        # #<digit> inside a block comment
        ("SELECT a FROM t /* see #123 */", "SELECT a FROM t /* see #123 */"),
        # No temp table at all
        ("SELECT a FROM t WHERE id > 0", "SELECT a FROM t WHERE id > 0"),
    ],
)
def test_sanitize_tsql_temp_tables(sql: str, expected: str) -> None:
    assert _sanitize_tsql_temp_tables(sql) == expected


def test_tsql_digit_leading_temp_table_lineage() -> None:
    """A #<digit> temp-table target must not blow up parsing and lose the real source.

    Without the sanitizer, sqlglot raises 'Expected table name but got NUMBER' and the
    whole statement's lineage (including the real FROM source) is lost.
    """
    resolver = SchemaResolver(platform="mssql")
    result = sqlglot_lineage(
        "SELECT a, b INTO #63114Actual FROM mydb.dbo.real_src",
        schema_resolver=resolver,
        override_dialect="mssql",
    )
    assert result.debug_info.table_error is None, result.debug_info.table_error
    assert any("real_src" in t for t in result.in_tables), result.in_tables
