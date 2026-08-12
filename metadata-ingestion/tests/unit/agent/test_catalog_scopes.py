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
    ("snowflake", "snowflake", "SELECT * FROM snowflake.account_usage.tables"),
    ("snowflake", "snowflake", "SELECT * FROM account_usage.object_dependencies"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.TABLES"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.COLUMNS"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.TABLE_OPTIONS"),
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
    # BigQuery extends INFORMATION_SCHEMA with JOBS, whose `query` column is the
    # SQL text of every job run in the project. A schema-level allow of
    # information_schema -- which is the framework default -- permits it, so this
    # is the case that forced BigQuery onto a named-relation allowlist.
    ("bigquery", "bigquery", "SELECT query FROM myds.INFORMATION_SCHEMA.JOBS"),
    (
        "bigquery",
        "bigquery",
        "SELECT query, user_email FROM myds.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
    ),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.JOBS_BY_USER"),
    ("snowflake", "snowflake", "SELECT query_text FROM account_usage.query_history"),
    (
        "snowflake",
        "snowflake",
        "SELECT * FROM snowflake.account_usage.access_history",
    ),
]

REFUSED_USER_DATA: List[Tuple[str, str, str]] = [
    ("mssql", "mssql", "SELECT * FROM dbo.orders"),
    ("postgres", "postgres", "SELECT * FROM public.customers"),
    ("oracle", "oracle", "SELECT * FROM hr.employees"),
    ("teradata", "teradata", "SELECT * FROM retail.orders"),
    ("clickhouse", "clickhouse", "SELECT * FROM analytics.events"),
]


def _scope(source_type: str) -> CatalogScope:
    """Resolve the scope the way _enforce_gates does, which is off the provider.

    Two homes, because there are two shapes. The SQLAlchemy family declares it on
    the config and for_config primes it onto the instance -- one provider class
    serves ~15 dialects, so the class cannot hold a per-dialect answer. A connector
    with its own provider class (Snowflake, BigQuery) declares it there instead.

    Read from the provider's own __dict__ rather than with getattr, so the base
    class's default does not shadow a config that declares one.
    """
    from datahub.ingestion.agent.probe_methods import _provider_class

    provider = _provider_class(source_type)
    declared = provider.__dict__.get("catalog_scope") if provider else None
    if isinstance(declared, CatalogScope):
        return declared
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


# Sources reviewed as safe on the bare default scope -- a schema-level allow of
# information_schema and nothing else. Safe here means: this dialect's
# information_schema holds schema shape only, with no view carrying the text of
# user queries.
#
# The list exists because that judgement cannot be made generically. A sweep
# matching relation names against a global list of text-bearing views was tried
# first and cried wolf: it flagged information_schema.jobs on Athena, where no
# such view exists. Whether a relation is text-bearing is irreducibly per-dialect,
# so the decision is recorded per source instead.
#
# BigQuery is the reason. It sat on this default for the whole of the branch, and
# BigQuery extends INFORMATION_SCHEMA with JOBS -- the SQL text of every job in the
# project. It now declares a named-relation allowlist and is absent from here.
_DEFAULT_SCOPE_REVIEWED = frozenset(
    {
        "athena",
        "db2",
        "doris",
        "druid",
        "hana",
        "hive",
        "hive-metastore",
        "mariadb",
        "mysql",
        "presto",
        "presto-on-hive",
        "sqlalchemy",
        "starburst-trino-usage",
        "starrocks",
        "tidb",
        "trino",
        "unity-catalog",
        "vertica",
    }
)


def test_a_source_on_the_default_scope_has_been_reviewed_for_it():
    """Force a decision when a connector inherits the bare default.

    Follows the pattern this repo already uses for sensitive config properties:
    rather than guess, require the classification to be explicit, and fail with
    instructions when something new appears.

    A named-relation allowlist is safe by construction -- nothing arrives
    permitted. A schema-level allow is a denylist, so somebody has to have looked
    at that dialect's information_schema and confirmed it carries no query text.
    """
    from datahub.ingestion.agent.probe_methods import _provider_class
    from datahub.ingestion.source.source_registry import source_registry

    default = CatalogScope()
    unreviewed: List[str] = []
    scanned = 0
    for source_type in sorted(source_registry.mapping):
        try:
            provider = _provider_class(source_type)
        except Exception:
            # Optional deps absent in this environment; the dialects that matter
            # here load on core deps alone (asserted below).
            continue
        if provider is None:
            continue
        scope = provider.__dict__.get("catalog_scope")
        if not isinstance(scope, CatalogScope):
            try:
                scope = config_class_for(source_type).probe_catalog_scope()
            except Exception:
                continue
        if not isinstance(scope, CatalogScope):
            continue
        scanned += 1
        if scope == default and source_type not in _DEFAULT_SCOPE_REVIEWED:
            unreviewed.append(source_type)

    assert scanned, "scanned no scopes at all, so this proved nothing"
    assert "mysql" in _DEFAULT_SCOPE_REVIEWED, "the sanity anchor went missing"
    assert not unreviewed, (
        "these sources inherit the bare information_schema default without having "
        "been reviewed for it. Check whether the dialect's information_schema "
        "carries user query text (BigQuery's JOBS does). If it does, declare a "
        "named-relation allowlist on the provider; if it does not, add the source "
        "to _DEFAULT_SCOPE_REVIEWED:\n  " + "\n  ".join(unreviewed)
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


def test_a_redshift_schema_verdict_reports_the_string_that_decided_it():
    """`target` must be what the pattern was matched against, or it misleads.

    Redshift matches "database.schema" once match_fully_qualified_names is on, and
    the probe used to report the bare name regardless. A caller then saw
    target='analytics' excluded by a pattern of '^analytics$' -- a verdict that
    contradicts its own explanation -- and would "fix" the pattern in the wrong
    direction. `target` is the one field probe filter exists to get right.
    """
    from datahub.ingestion.agent.filter_check import check_filters

    base: Dict[str, Any] = {
        "host_port": "h:5439",
        "database": "dev",
        "username": "u",
        "password": "p",
        "match_fully_qualified_names": True,
    }

    matched = check_filters(
        source_type="redshift",
        config_dict={**base, "schema_pattern": {"allow": [r"^dev\.analytics$"]}},
        kind="Schema",
        parent_path=[],
        names=["analytics"],
    ).results[0]
    assert matched.target == "dev.analytics"
    assert matched.included is True

    # The bare name does not match, and the report says so against the same target.
    missed = check_filters(
        source_type="redshift",
        config_dict={**base, "schema_pattern": {"allow": ["^analytics$"]}},
        kind="Schema",
        parent_path=[],
        names=["analytics"],
    ).results[0]
    assert missed.target == "dev.analytics"
    assert missed.included is False
    assert missed.excluded_by == "schema_pattern"


def test_without_the_flag_redshift_matches_the_bare_schema_name():
    from datahub.ingestion.agent.filter_check import check_filters

    result = check_filters(
        source_type="redshift",
        config_dict={
            "host_port": "h:5439",
            "database": "dev",
            "username": "u",
            "password": "p",
            "schema_pattern": {"allow": ["^analytics$"]},
        },
        kind="Schema",
        parent_path=[],
        names=["analytics"],
    ).results[0]
    assert result.target == "analytics"
    assert result.included is True
