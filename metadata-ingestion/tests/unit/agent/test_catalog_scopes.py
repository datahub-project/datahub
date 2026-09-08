"""Each dialect's catalog surface, as its own connector declares it.

The gate used to hold this centrally and got it wrong: Oracle and Teradata have no
`information_schema` at all, so both advertised a `sql` command whose every
legitimate query was refused. The cases below are drawn from what our own ingestion
code reads -- DBC.TablesV, DBA_TABLES, sys.tables -- and from the query-text
surfaces sitting beside them in the same schemas.
"""

from typing import Any, Dict, Iterator, List, Tuple

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import _provider_class, config_class_for
from datahub.ingestion.agent.sql_gate import (
    INFORMATION_SCHEMA,
    CatalogScope,
    SqlScopeError,
    check_query_scope,
)
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.source.sql.sqlalchemy_probe import sqlglot_dialect_for

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
    (
        "snowflake",
        "snowflake",
        "SELECT * FROM snowflake.account_usage.object_dependencies",
    ),
    ("redshift", "redshift", "SELECT * FROM pg_catalog.svv_redshift_schemas"),
    ("redshift", "redshift", "SELECT * FROM pg_catalog.pg_class"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.TABLES"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.COLUMNS"),
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.TABLE_OPTIONS"),
    # Read by each source's own queries, and refused by the first cut of these
    # declarations. Naming too few relations fails as surely as naming too
    # many: the probe exists to show what the recipe will see.
    ("redshift", "redshift", "SELECT datname FROM pg_catalog.pg_database"),
    ("redshift", "redshift", "SELECT * FROM pg_catalog.svv_redshift_columns"),
    ("redshift", "redshift", "SELECT * FROM pg_catalog.svv_external_tables"),
    ("redshift", "redshift", "SELECT attname FROM pg_catalog.pg_attribute"),
    ("mssql", "mssql", "SELECT * FROM sys.sql_expression_dependencies"),
    ("mssql", "mssql", "SELECT * FROM sys.database_query_store_options"),
    # BigQuery is the one dialect whose parser leaves a dot inside an
    # identifier slot, so it is the only one whose slots get split. These must
    # keep working after that split was narrowed to it.
    ("bigquery", "bigquery", "SELECT * FROM myds.INFORMATION_SCHEMA.VIEWS"),
    # A CTE is still excused where an enclosing WITH declares it, including a
    # later CTE referring to an earlier sibling.
    (
        "postgres",
        "postgres",
        "WITH t AS (SELECT * FROM information_schema.tables) SELECT * FROM t",
    ),
    (
        "postgres",
        "postgres",
        "WITH a AS (SELECT * FROM information_schema.tables), "
        "b AS (SELECT * FROM a) SELECT * FROM b",
    ),
    # Postgres moved from a schema-level pg_catalog allow to named relations;
    # these are what its own query.py/source.py read.
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_depend"),
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_rewrite"),
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_proc"),
    ("postgres", "postgres", "SELECT attname FROM pg_catalog.pg_attribute"),
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_views"),
]

# The text-bearing relation that sits in the same catalog as the ones above. Each of
# these is read by our own ingestion for usage or lineage, so none is hypothetical.
REFUSED_QUERY_TEXT: List[Tuple[str, str, str]] = [
    ("mssql", "mssql", "SELECT definition FROM sys.sql_modules"),
    ("mssql", "mssql", "SELECT plan FROM sys.dm_exec_cached_plans"),
    # Query Store's *configuration* is permitted above; its captured text is
    # not, and the two sit one name apart.
    ("mssql", "mssql", "SELECT query_sql_text FROM sys.query_store_query_text"),
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
    # Redshift keeps its executed SQL in pg_catalog alongside the svv_* metadata
    # views. The first version of this declaration allowed pg_catalog at schema
    # level *and* listed relations, believing the list was what kept these out --
    # but permits() short-circuits on the schema, so all three were readable.
    ("redshift", "redshift", "SELECT querytxt FROM pg_catalog.stl_query"),
    ("redshift", "redshift", "SELECT text FROM pg_catalog.stl_querytext"),
    ("redshift", "redshift", "SELECT text FROM pg_catalog.svl_statementtext"),
    # Postgres holds the text of prepared statements here. pg_stat_statements and
    # pg_stat_activity were excluded; this one was missed.
    (
        "postgres",
        "postgres",
        "SELECT statement FROM pg_catalog.pg_prepared_statements",
    ),
]

# A catalog-qualified relation must pin its catalog. Snowflake's ACCOUNT_USAGE is a
# schema inside the SNOWFLAKE database, but nothing stops a user creating their own
# database with a schema of the same name -- and a scope that only ever compares the
# last two path segments would read that user's tables as though they were the
# system view.
REFUSED_CATALOG_IMPERSONATION: List[Tuple[str, str, str]] = [
    ("snowflake", "snowflake", "SELECT * FROM attacker_db.account_usage.tables"),
    ("snowflake", "snowflake", "SELECT * FROM my_db.ACCOUNT_USAGE.COLUMNS"),
    # Two parts alone cannot be shown to be the system schema either: an
    # unqualified reference resolves against whatever database is current.
    ("snowflake", "snowflake", "SELECT * FROM account_usage.tables"),
    # The same mistake in the other direction, and the one suffix matching
    # introduced: Oracle lists its dictionary views bare, because they are
    # public synonyms read unqualified. Suffix-matching a bare entry licensed
    # the name under every schema, so a user table called all_tables read as
    # catalog metadata. Bare entries belong to permits_unqualified alone.
    ("oracle", "oracle", "SELECT * FROM hr.all_tables"),
    ("oracle", "oracle", "SELECT * FROM my_schema.dba_tables"),
    ("oracle", "oracle", "SELECT * FROM some_db.hr.all_tab_columns"),
    # A quoted identifier is ONE name, even when it contains dots. Splitting
    # every slot on "." -- an accommodation BigQuery needs, applied to every
    # dialect -- let a user table named after a catalog view read as that view,
    # on every dialect at once. The same substitution as the ACCOUNT_USAGE
    # cases above, one level lower down.
    ("postgres", "postgres", 'SELECT * FROM "information_schema.tables"'),
    ("mysql", "mysql", "SELECT * FROM `information_schema.tables`"),
    ("snowflake", "snowflake", 'SELECT * FROM "snowflake.account_usage.tables"'),
    ("mssql", "mssql", 'SELECT * FROM "sys.tables"'),
    # CTE names were collected for the whole statement, so a CTE in an inner,
    # non-enclosing scope licensed its name in an outer FROM -- where SQL
    # itself resolves the name to the real table. Postgres does not make an
    # inner WITH visible to an outer FROM.
    (
        "postgres",
        "postgres",
        "SELECT * FROM customer_pii WHERE 1 IN "
        "(WITH customer_pii AS (SELECT 1 AS a) SELECT a FROM customer_pii)",
    ),
]

# Relations that are not query text and not schema shape either: sampled row
# values, raw object bytes, credentials, user identity. Each of these was
# permitted by a schema-level allow whose exclusion list was incomplete -- the
# hazard CatalogScope's own docstring argues against.
REFUSED_NOT_SCHEMA_SHAPE: List[Tuple[str, str, str]] = [
    # most_common_vals / histogram_bounds are literal sampled values out of
    # user columns -- the row values themselves, not values inside a query.
    ("postgres", "postgres", "SELECT most_common_vals FROM pg_catalog.pg_stats"),
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_statistic"),
    # Raw bytes of user large objects.
    ("postgres", "postgres", "SELECT data FROM pg_catalog.pg_largeobject"),
    ("postgres", "postgres", "SELECT * FROM pg_catalog.pg_largeobject_metadata"),
    # Role password hashes.
    ("postgres", "postgres", "SELECT passwd FROM pg_catalog.pg_shadow"),
    ("postgres", "postgres", "SELECT rolpassword FROM pg_catalog.pg_authid"),
]

# User identity, withheld consistently across every dialect. Oracle's
# all_users was the one that had been permitted.
REFUSED_USER_IDENTITY: List[Tuple[str, str, str]] = [
    ("oracle", "oracle", "SELECT username FROM all_users"),
    ("postgres", "postgres", "SELECT usename FROM pg_catalog.pg_user"),
    ("postgres", "postgres", "SELECT rolname FROM pg_catalog.pg_roles"),
    ("clickhouse", "clickhouse", "SELECT name FROM system.users"),
    ("teradata", "teradata", "SELECT UserName FROM DBC.UsersV"),
    ("mssql", "mssql", "SELECT name FROM sys.sql_logins"),
]


@pytest.mark.parametrize("source_type,platform,query", REFUSED_NOT_SCHEMA_SHAPE)
def test_a_relation_that_is_not_schema_shape_is_refused(
    source_type: str, platform: str, query: str
) -> None:
    with pytest.raises(SqlScopeError):
        check_query_scope(query, platform=platform, scope=_scope(source_type))


@pytest.mark.parametrize("source_type,platform,query", REFUSED_USER_IDENTITY)
def test_user_identity_is_withheld_on_every_dialect(
    source_type: str, platform: str, query: str
) -> None:
    with pytest.raises(SqlScopeError):
        check_query_scope(query, platform=platform, scope=_scope(source_type))


@pytest.mark.parametrize("source_type,platform,query", REFUSED_CATALOG_IMPERSONATION)
def test_a_lookalike_schema_in_another_catalog_is_refused(
    source_type: str, platform: str, query: str
) -> None:
    with pytest.raises(SqlScopeError):
        check_query_scope(query, platform=platform, scope=_scope(source_type))


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


def _declared_scopes() -> Iterator[Tuple[str, CatalogScope]]:
    """Every source's declared scope, resolved the way _enforce_gates resolves it.

    Two homes, because there are two shapes: the SQLAlchemy family declares it on
    the config, a connector with its own provider class declares it there. Shared
    by the two registry-wide scans below so they cannot drift apart on which
    sources they cover. Sources whose optional deps are absent are skipped -- the
    dialects that matter here load on core deps alone, which the scans assert.
    """

    for source_type in sorted(source_registry.mapping):
        try:
            provider = _provider_class(source_type)
        except Exception:
            continue
        if provider is None:
            continue
        # Read from the provider's own __dict__ rather than with getattr, so the
        # base class's default does not shadow a config that declares one.
        scope = provider.__dict__.get("catalog_scope")
        if not isinstance(scope, CatalogScope):
            try:
                scope = config_class_for(source_type).probe_catalog_scope()
            except Exception:
                continue
        if isinstance(scope, CatalogScope):
            yield source_type, scope


def test_a_source_on_the_default_scope_has_been_reviewed_for_it():
    """Force a decision when a connector inherits the bare default.

    Follows the pattern this repo already uses for sensitive config properties:
    rather than guess, require the classification to be explicit, and fail with
    instructions when something new appears.

    A named-relation allowlist is safe by construction -- nothing arrives
    permitted. A schema-level allow is a denylist, so somebody has to have looked
    at that dialect's information_schema and confirmed it carries no query text.
    """
    default = CatalogScope()
    unreviewed: List[str] = []
    scanned = 0
    for source_type, scope in _declared_scopes():
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


def test_every_declared_relation_does_work():
    """A scope must not list a relation that changes nothing.

    This is the shape of the original Redshift bug: pg_catalog was allowed at
    schema level *and* relations were listed, with a comment claiming the list
    was what kept the query-text views out. It was not -- the schema allow
    short-circuits first, so every entry in that list was dead code, and
    nothing said so.

    Two checks, and both can actually fire. An earlier version of this test also
    asked whether the matcher could reach each entry, which was tautological: a
    qualified entry always matches its own suffix, so that branch could never
    fail, and the typo detection it advertised was imaginary. Checking the
    entry's shape directly is what catches a malformed one.
    """
    problems: List[str] = []
    checked = 0
    for source_type, scope in _declared_scopes():
        if not scope.relations:
            continue
        allowed_schemas = {s.lower() for s in scope.schemas}
        for entry in sorted(scope.relations):
            checked += 1
            parts = entry.split(".")
            # An empty or padded segment names something no reference can be:
            # "pg_catalog..pg_class", "sys. tables". A non-dot separator
            # collapses the whole entry into one segment holding punctuation.
            if any(part.strip() != part or not part for part in parts):
                problems.append(
                    f"{source_type}: {entry!r} has an empty or padded segment, "
                    f"so no reference can match it"
                )
                continue
            if any(ch in entry for ch in "/\\ \t"):
                problems.append(f"{source_type}: {entry!r} is not dot-separated")
                continue
            # The dead-list check, judged against the scope's own schemas: an
            # entry whose schema is allowed wholesale restricts nothing while
            # reading as though it did.
            if len(parts) >= 2 and parts[-2].lower() in allowed_schemas:
                problems.append(
                    f"{source_type}: {entry} is shadowed by the '{parts[-2]}' "
                    f"schema-level allow, so it restricts nothing"
                )

    assert checked, "checked no relations at all, so this proved nothing"
    assert not problems, (
        "these relations are listed but do no work. Either the entry is "
        "malformed, or its schema is allowed wholesale and the entry only reads "
        "as though it narrowed something:\n  " + "\n  ".join(problems)
    )


def test_the_postgres_declaration_is_inherited_by_its_derivatives():
    # One declaration covers three connectors; CockroachDB and TimescaleDB extend
    # PostgresConfig rather than restating it. Asserted on the named relations
    # and on what they withhold, since pg_catalog is no longer allowed at schema
    # level -- the derivatives must inherit the narrowing, not just the allowing.
    for source_type in ("postgres", "cockroachdb", "timescaledb"):
        scope = _scope(source_type)
        assert "pg_catalog.pg_class" in scope.relations
        assert "pg_catalog" not in scope.schemas
        assert not scope.permits_path(["pg_catalog", "pg_shadow"])
        assert scope.permits_path(["pg_catalog", "pg_class"])


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
