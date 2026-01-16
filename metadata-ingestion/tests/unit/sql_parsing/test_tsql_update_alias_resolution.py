"""Tests for TSQL UPDATE alias resolution.

TSQL (SQL Server) uses a unique syntax where UPDATE target can be an alias:
    UPDATE dst SET ... FROM dbo.target_table dst JOIN ...

This is different from other databases where the table name is required.
These tests verify that the alias is correctly resolved to the real table.
"""

import pathlib

import pytest

from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage
from datahub.testing.check_sql_parser_result import assert_sql_result

RESOURCE_DIR = pathlib.Path(__file__).parent / "goldens"


@pytest.fixture(scope="function", autouse=True)
def _disable_cooperative_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.SQL_LINEAGE_TIMEOUT_ENABLED", False
    )


def test_tsql_update_with_alias_basic() -> None:
    """TSQL UPDATE with alias should resolve to real table."""
    assert_sql_result(
        """
UPDATE dst
SET dst.col1 = src.col1
FROM mydb.dbo.target_table dst
JOIN mydb.dbo.source_table src ON dst.id = src.id
""",
        dialect="tsql",
        default_db="mydb",
        default_schema="dbo",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:tsql,mydb.dbo.target_table,PROD)": {
                "id": "int",
                "col1": "varchar",
            },
            "urn:li:dataset:(urn:li:dataPlatform:tsql,mydb.dbo.source_table,PROD)": {
                "id": "int",
                "col1": "varchar",
            },
        },
        expected_file=RESOURCE_DIR / "test_tsql_update_with_alias_basic.json",
    )


def test_tsql_update_without_alias() -> None:
    """TSQL UPDATE without alias should work unchanged."""
    assert_sql_result(
        """
UPDATE mydb.dbo.target_table
SET col1 = 'value'
WHERE id = 1
""",
        dialect="tsql",
        default_db="mydb",
        default_schema="dbo",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:tsql,mydb.dbo.target_table,PROD)": {
                "id": "int",
                "col1": "varchar",
            },
        },
        expected_file=RESOURCE_DIR / "test_tsql_update_without_alias.json",
    )


def test_tsql_update_with_subquery() -> None:
    """TSQL UPDATE with alias and subquery - verifies same-name alias bug fix.

    After qualify(), sqlglot may add 'FROM table AS table'. This test ensures
    tables inside subqueries are NOT incorrectly filtered when alias equals
    table name.
    """
    assert_sql_result(
        """
UPDATE dst
SET dst.col1 = sub.max_val
FROM mydb.dbo.target_table dst
JOIN (SELECT id, MAX(value) as max_val FROM mydb.dbo.source_table GROUP BY id) sub
    ON dst.id = sub.id
""",
        dialect="tsql",
        default_db="mydb",
        default_schema="dbo",
        schemas={
            "urn:li:dataset:(urn:li:dataPlatform:tsql,mydb.dbo.target_table,PROD)": {
                "id": "int",
                "col1": "int",
            },
            "urn:li:dataset:(urn:li:dataPlatform:tsql,mydb.dbo.source_table,PROD)": {
                "id": "int",
                "value": "int",
            },
        },
        expected_file=RESOURCE_DIR / "test_tsql_update_with_subquery.json",
    )


# Additional tests using inline assertions (no golden files needed)


def test_tsql_update_cross_db_resolves_alias() -> None:
    """TSQL UPDATE with alias across databases resolves correctly."""
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    schema_resolver = SchemaResolver(
        platform="mssql",
        env="PROD",
    )
    result = sqlglot_lineage(
        """
UPDATE dst
SET dst.col1 = src.col1
FROM TimeSeries.dbo.target_table dst
JOIN Staging.dbo.source_table src ON dst.id = src.id
""",
        schema_resolver=schema_resolver,
        default_db="TimeSeries",
        default_schema="dbo",
    )
    # Alias 'dst' should resolve to real table, not appear as separate table
    out_tables = [str(t) for t in result.out_tables]
    assert any("target_table" in t for t in out_tables)
    assert len(out_tables) == 1


def test_tsql_update_mixed_case_alias_resolves() -> None:
    """TSQL UPDATE with mixed-case alias uses case-insensitive matching."""
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    schema_resolver = SchemaResolver(
        platform="mssql",
        env="PROD",
    )
    result = sqlglot_lineage(
        """
UPDATE Dst
SET Dst.col1 = src.col1
FROM mydb.dbo.target_table DST
JOIN mydb.dbo.source_table src ON DST.id = src.id
""",
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="dbo",
    )
    # Mixed case 'Dst'/'DST' should resolve to target_table
    out_tables = [str(t) for t in result.out_tables]
    assert any("target_table" in t for t in out_tables)
    assert len(out_tables) == 1


def test_tsql_update_same_alias_in_subquery_scope() -> None:
    """Same alias in subquery should not conflict with outer scope.

    This tests a potential bug where 'dst' alias is used both in the outer
    FROM clause and inside a subquery. The outer 'dst' should be resolved
    correctly without being confused with the inner scoped 'dst'.
    """
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    schema_resolver = SchemaResolver(
        platform="mssql",
        env="PROD",
    )
    result = sqlglot_lineage(
        """
UPDATE dst
SET dst.col1 = sub.val
FROM target_table dst
JOIN (SELECT id, val FROM source_table dst GROUP BY id, val) sub
    ON dst.id = sub.id
""",
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="dbo",
    )
    # 'dst' should resolve to outer target_table, not inner source_table
    out_tables = [str(t) for t in result.out_tables]
    assert any("target_table" in t for t in out_tables)
    assert len(out_tables) == 1
    # source_table should be in in_tables (from subquery)
    in_tables = [str(t) for t in result.in_tables]
    assert any("source_table" in t for t in in_tables)


def test_tsql_delete_with_alias_filters_correctly() -> None:
    """TSQL DELETE with alias should not have spurious alias in in_tables.

    DELETE dst FROM target_table dst WHERE ... should:
    - Have target_table in out_tables
    - NOT have 'dst' as a spurious entry in in_tables
    """
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    schema_resolver = SchemaResolver(
        platform="mssql",
        env="PROD",
    )
    result = sqlglot_lineage(
        """
DELETE dst
FROM target_table dst
WHERE dst.id IN (SELECT id FROM to_delete)
""",
        schema_resolver=schema_resolver,
        default_db="mydb",
        default_schema="dbo",
    )
    # out_tables should have target_table
    out_tables = [str(t) for t in result.out_tables]
    assert any("target_table" in t for t in out_tables)
    assert len(out_tables) == 1
    # in_tables should have to_delete but NOT 'dst' as a spurious table
    in_tables = [str(t) for t in result.in_tables]
    assert any("to_delete" in t for t in in_tables)
    # Verify no spurious 'dst' table (would appear as mydb.dbo.dst)
    assert not any("dbo.dst" in t.lower() for t in in_tables)
