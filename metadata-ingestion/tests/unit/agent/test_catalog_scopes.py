"""Each dialect's catalog surface, as its own connector declares it.

The gate used to hold this centrally and got it wrong: Oracle and Teradata have no
`information_schema` at all, so both advertised a `sql` command whose every
legitimate query was refused. The cases below are drawn from what our own ingestion
code reads -- DBC.TablesV, DBA_TABLES, sys.tables -- and from the query-text
surfaces sitting beside them in the same schemas.
"""

from typing import Any, Dict, List, Tuple

import pytest

from datahub.ingestion.agent.probe_methods import config_class_for
from datahub.ingestion.agent.sql_gate import (
    INFORMATION_SCHEMA,
    CatalogScope,
    SqlScopeError,
    check_query_scope,
)

# (source_type, sqlglot platform, query, should the gate permit it)
PERMITTED: List[Tuple[str, str, str]] = [
    ("mssql", "mssql", "SELECT name FROM sys.tables"),
    ("mssql", "mssql", "SELECT name FROM sys.columns"),
    ("mssql", "mssql", "SELECT table_name FROM information_schema.tables"),
    ("teradata", "teradata", "SELECT TableName FROM DBC.TablesV"),
    ("teradata", "teradata", "SELECT ColumnName FROM DBC.ColumnsV"),
    ("oracle", "oracle", "SELECT table_name FROM all_tables"),
    ("oracle", "oracle", "SELECT column_name FROM dba_tab_columns"),
    ("clickhouse", "clickhouse", "SELECT name FROM system.tables"),
    ("clickhouse", "clickhouse", "SELECT table_name FROM information_schema.tables"),
    ("postgres", "postgres", "SELECT relname FROM pg_catalog.pg_class"),
    ("redshift", "redshift", "SELECT * FROM pg_catalog.svv_table_info"),
    ("mysql", "mysql", "SELECT table_name FROM information_schema.tables"),
    ("snowflake", "snowflake", "SELECT table_name FROM information_schema.tables"),
]

# The text-bearing relation that sits in the same catalog as the ones above. Each of
# these is read by our own ingestion for usage or lineage, so none is hypothetical.
REFUSED_QUERY_TEXT: List[Tuple[str, str, str]] = [
    ("mssql", "mssql", "SELECT definition FROM sys.sql_modules"),
    ("mssql", "mssql", "SELECT plan FROM sys.dm_exec_cached_plans"),
    ("teradata", "teradata", "SELECT QueryText FROM DBC.QryLogV"),
    ("oracle", "oracle", "SELECT text FROM dba_source"),
    ("clickhouse", "clickhouse", "SELECT query FROM system.query_log"),
    ("postgres", "postgres", "SELECT query FROM pg_catalog.pg_stat_statements"),
    ("postgres", "postgres", "SELECT query FROM pg_catalog.pg_stat_activity"),
]

REFUSED_USER_DATA: List[Tuple[str, str, str]] = [
    ("mssql", "mssql", "SELECT * FROM dbo.orders"),
    ("postgres", "postgres", "SELECT * FROM public.customers"),
    ("oracle", "oracle", "SELECT * FROM hr.employees"),
    ("teradata", "teradata", "SELECT * FROM retail.orders"),
    ("clickhouse", "clickhouse", "SELECT * FROM analytics.events"),
]


def _scope(source_type: str) -> CatalogScope:
    return config_class_for(source_type).probe_catalog_scope()


@pytest.mark.parametrize("source_type,platform,query", PERMITTED)
def test_a_dialects_own_catalog_read_is_permitted(
    source_type: str, platform: str, query: str
) -> None:
    check_query_scope(query, platform=platform, scope=_scope(source_type))


@pytest.mark.parametrize("source_type,platform,query", REFUSED_QUERY_TEXT)
def test_query_text_inside_a_catalog_is_still_refused(
    source_type: str, platform: str, query: str
) -> None:
    # The reason relations are named rather than whole schemas: each of these lives
    # in a schema whose other relations are metadata, and carries WHERE-clause
    # literals from user queries.
    with pytest.raises(SqlScopeError):
        check_query_scope(query, platform=platform, scope=_scope(source_type))


@pytest.mark.parametrize("source_type,platform,query", REFUSED_USER_DATA)
def test_user_data_is_refused_whatever_the_dialect(
    source_type: str, platform: str, query: str
) -> None:
    with pytest.raises(SqlScopeError, match="outside the catalog metadata"):
        check_query_scope(query, platform=platform, scope=_scope(source_type))


@pytest.mark.parametrize("platform", ["db2", "vertica"])
def test_dialects_the_gate_cannot_resolve(platform: str) -> None:
    """sqlglot has no dialect for these, so `sql` fails closed on every query.

    Correct -- parsing against a near-enough grammar could clear a reference it had
    misread -- but it means the connector's `sql` command cannot work at all, whatever
    catalog it declares. That is why db2.py deliberately declares no scope. If sqlglot
    gains either dialect, this test fails and the declaration becomes worth adding.
    """
    with pytest.raises(SqlScopeError, match="cannot resolve a SQL dialect"):
        check_query_scope("SELECT tabname FROM syscat.tables", platform=platform)


def test_cockroachdb_is_parsed_as_postgres_because_that_is_what_it_speaks():
    # Not a near-enough guess: CockroachDB implements the Postgres wire protocol and
    # dialect. Before the mapping its sql command failed closed on every query.
    from datahub.ingestion.source.sql.sqlalchemy_probe import sqlglot_dialect_for

    assert sqlglot_dialect_for("cockroachdb") == "postgres"
    check_query_scope(
        "SELECT relname FROM pg_catalog.pg_class",
        platform=sqlglot_dialect_for("cockroachdb"),
        scope=_scope("cockroachdb"),
    )


def test_the_postgres_declaration_is_inherited_by_its_derivatives():
    # One declaration covers three connectors; CockroachDB and TimescaleDB extend
    # PostgresConfig rather than restating it.
    for source_type in ("postgres", "cockroachdb", "timescaledb"):
        assert "pg_catalog" in _scope(source_type).schemas


def test_the_default_is_information_schema_and_nothing_else():
    # What a connector that declares nothing gets: safe everywhere, and enough for a
    # standard dialect.
    scope = CatalogScope()
    assert scope.schemas == frozenset({INFORMATION_SCHEMA})
    assert scope.relations == frozenset()


def test_a_relation_outside_a_permitted_schema_needs_naming_individually():
    scope = CatalogScope(relations=frozenset({"sys.tables"}))
    assert scope.permits("sys", "tables")
    assert not scope.permits("sys", "sql_modules")
    # And schema-level allow still covers everything inside it.
    assert scope.permits(INFORMATION_SCHEMA, "anything_at_all")


def test_matching_ignores_case_because_dialects_disagree_about_it():
    scope = CatalogScope(schemas=frozenset(), relations=frozenset({"DBC.TablesV"}))
    assert scope.permits("dbc", "tablesv")
    assert scope.permits("DBC", "TABLESV")


def test_only_a_listed_bare_name_may_go_unqualified():
    scope = CatalogScope(relations=frozenset({"all_tables", "sys.tables"}))
    assert scope.permits_unqualified("all_tables")
    # A schema-qualified entry does not license the bare relation name, or every
    # `FROM tables` would read as sys.tables.
    assert not scope.permits_unqualified("tables")


def test_the_refusal_says_what_this_source_does_permit():
    # A refusal is the caller's only signal for how to rewrite.
    scope = CatalogScope(relations=frozenset({"sys.tables", "sys.columns"}))
    with pytest.raises(SqlScopeError, match="information_schema.*2 individually"):
        check_query_scope("SELECT * FROM dbo.orders", platform="mssql", scope=scope)


def test_declared_scopes_carry_no_user_schema():
    """No declaration may open a schema where user tables live.

    The exposure this mechanism creates: pushing the catalog surface to connectors
    means a careless declaration can widen it. A schema like `public` or `dbo` holds
    user tables by convention, so naming one turns the gate into theatre.
    """
    forbidden = {"public", "dbo", "main", "default", "user", "sys", "system", "dbc"}
    offenders: Dict[str, Any] = {}
    for source_type in (
        "postgres",
        "redshift",
        "mssql",
        "oracle",
        "teradata",
        "clickhouse",
        "mysql",
        "snowflake",
        "bigquery",
    ):
        named = {s.lower() for s in _scope(source_type).schemas} & forbidden
        if named:
            offenders[source_type] = sorted(named)
    assert offenders == {}, (
        "these declarations allow a whole schema that holds user tables (or a vendor "
        f"catalog that holds query text); name relations instead: {offenders}"
    )
