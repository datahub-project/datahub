import pytest

from datahub.ingestion.agent.sql_gate import SqlScopeError, check_query_scope

CATALOG_QUERY = (
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
)


def test_permits_a_catalog_only_select():
    check_query_scope(CATALOG_QUERY, platform="postgres")


def test_permits_a_catalog_reference_qualified_by_database():
    check_query_scope(
        "SELECT table_name FROM mydb.information_schema.tables", platform="postgres"
    )


def test_permits_pg_catalog_on_postgres():
    check_query_scope("SELECT relname FROM pg_catalog.pg_class", platform="postgres")


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
    # queries, including values in WHERE clauses. Living in a catalog schema does
    # not make it metadata.
    with pytest.raises(SqlScopeError, match="(?i)account_usage"):
        check_query_scope(
            "SELECT query_text FROM snowflake.account_usage.query_history",
            platform="snowflake",
        )


def test_rejects_pg_stat_statements():
    # The Postgres analogue of query history: normalized query text, still
    # carrying literals in many configurations.
    with pytest.raises(SqlScopeError, match="(?i)pg_stat_statements"):
        check_query_scope(
            "SELECT query FROM pg_catalog.pg_stat_statements", platform="postgres"
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
