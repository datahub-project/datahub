"""Tests for Redshift query generation and validation.

Covers query patterns used across RedshiftProvisionedQuery and RedshiftServerlessQuery,
including segment stitching strategies that preserve word boundaries when reconstructing
queries from fixed-width character segments (200 bytes provisioned, 4000 bytes serverless).
"""

from datetime import datetime
from typing import Dict, List

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

from datahub.ingestion.source.redshift.query import (
    RedshiftCommonQuery,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
    redshift_datetime_format,
)

START_TIME = datetime(2024, 1, 1, 12, 0, 0)
END_TIME = datetime(2024, 1, 10, 12, 0, 0)

# Enough of the Redshift system catalog for sqlglot to resolve every column these
# builders reference. Only the log/catalog tables the lineage queries touch.
# Typed as sqlglot's own `schema` parameter expects (dict values are invariant).
REDSHIFT_SYSTEM_SCHEMA: Dict[str, object] = {
    "stl_querytext": {
        "userid": "INT",
        "xid": "BIGINT",
        "pid": "INT",
        "query": "INT",
        "sequence": "INT",
        "text": "VARCHAR",
    },
    "stl_query": {
        "userid": "INT",
        "query": "INT",
        "label": "VARCHAR",
        "xid": "BIGINT",
        "pid": "INT",
        "database": "VARCHAR",
        "querytxt": "VARCHAR",
        "starttime": "TIMESTAMP",
        "endtime": "TIMESTAMP",
        "aborted": "INT",
    },
    "stl_insert": {
        "userid": "INT",
        "query": "INT",
        "slice": "INT",
        "segment": "INT",
        "step": "INT",
        "starttime": "TIMESTAMP",
        "endtime": "TIMESTAMP",
        "tbl": "INT",
        "rows": "BIGINT",
    },
    "stl_scan": {
        "userid": "INT",
        "query": "INT",
        "slice": "INT",
        "segment": "INT",
        "step": "INT",
        "starttime": "TIMESTAMP",
        "endtime": "TIMESTAMP",
        "tbl": "INT",
        "type": "INT",
        "rows": "BIGINT",
    },
    "stl_load_commits": {"userid": "INT", "query": "INT", "slice": "INT"},
    "svv_table_info": {
        "database": "VARCHAR",
        "schema": "VARCHAR",
        "table_id": "INT",
        "table": "VARCHAR",
        "size": "BIGINT",
        "tbl_rows": "BIGINT",
    },
    "svl_user_info": {"usesysid": "INT", "usename": "VARCHAR"},
    "sys_query_detail": {
        "user_id": "INT",
        "query_id": "BIGINT",
        "table_id": "INT",
        "step_name": "VARCHAR",
        "source": "VARCHAR",
        "start_time": "TIMESTAMP",
        "end_time": "TIMESTAMP",
    },
    "sys_query_text": {
        "query_id": "BIGINT",
        "sequence": "INT",
        "text": "VARCHAR",
        "session_id": "INT",
    },
    "svv_user_info": {"user_id": "INT", "user_name": "VARCHAR"},
    "sys_load_detail": {"query_id": "BIGINT"},
}


def assert_valid_redshift_sql(sql: str, expected_ctes: List[str]) -> None:
    """Resolve every column reference and check CTE declaration order.

    A CTE projection missing a column its outer SELECT references parses cleanly and
    fails only on a live cluster; qualify() reports `Unknown column`. A non-recursive
    WITH can only reference CTEs declared before it, so `expected_ctes` asserts
    correctness rather than style.
    """
    # The bound parameters are placeholders, not SQL; give them a literal to parse.
    parsed = sqlglot.parse_one(sql.replace("%s", "'?'"), dialect="redshift")

    assert [cte.alias for cte in parsed.find_all(exp.CTE)] == expected_ctes

    # Given a table it has no entry for, qualify() infers a schema rather than failing,
    # which turns column resolution off for that table with no signal -- a misspelled
    # table name would take every column on it out of the check. Neither the default nor
    # infer_schema=False raises, so require the entry instead.
    referenced = {table.name.lower() for table in parsed.find_all(exp.Table)}
    cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}
    unregistered = referenced - cte_names - set(REDSHIFT_SYSTEM_SCHEMA)
    assert not unregistered, (
        f"{sorted(unregistered)} absent from REDSHIFT_SYSTEM_SCHEMA, so their columns "
        "would go unchecked; add them"
    )

    # qualify() rewrites in place, so resolve only after reading the CTE order.
    qualify(parsed, schema=REDSHIFT_SYSTEM_SCHEMA, dialect="redshift")


def assert_query_text_scoped(sql: str, table: str) -> None:
    """Assert every scan of `table` is restricted by a subquery predicate.

    Counting occurrences of the predicate text cannot see which scan carries it, so a
    query that drops the scope from the query-text table and gains a redundant one
    elsewhere keeps the same count. Locating the scan and inspecting its own WHERE
    binds the assertion to the scan that matters, and leaves it indifferent to `IN`
    versus `EXISTS`, to casing and wrapping, and to the driving CTE's name.
    """
    parsed = sqlglot.parse_one(sql.replace("%s", "'?'"), dialect="redshift")

    scans = [t for t in parsed.find_all(exp.Table) if t.name.lower() == table.lower()]
    assert scans, f"{table} is not scanned by this query"

    for scan in scans:
        select = scan.find_ancestor(exp.Select)
        where = select and select.args.get("where")
        assert where is not None, f"the {table} scan has no WHERE clause"
        assert any(
            predicate.find(exp.Select) is not None
            for predicate in where.find_all(exp.In, exp.Exists)
        ), f"the {table} scan is not restricted by a subquery predicate"


# The boundary-aware LISTAGG pattern for 200-byte segments (provisioned).
# Appends a space when the trimmed segment is shorter than the segment size,
# indicating a word boundary was at the segment edge.
PROVISIONED_LISTAGG_PATTERN = (
    "RTRIM(LISTAGG(RTRIM(text) "
    "|| CASE WHEN LEN(RTRIM(text)) < 200 THEN ' ' ELSE '' END, '')"
)

# The boundary-aware LISTAGG pattern for 4000-byte segments (serverless).
SERVERLESS_LISTAGG_PATTERN_TEXT = (
    'RTRIM(LISTAGG(RTRIM(qt."text") '
    "|| CASE WHEN LEN(RTRIM(qt.\"text\")) < 4000 THEN ' ' ELSE '' END, '')"
)

SERVERLESS_LISTAGG_PATTERN_QUERYTXT = (
    "RTRIM(LISTAGG(RTRIM(querytxt) "
    "|| CASE WHEN LEN(RTRIM(querytxt)) < 4000 THEN ' ' ELSE '' END, '')"
)


class TestCommonQueries:
    def test_list_schemas_without_ownership_uses_null_owner(self):
        sql = RedshiftCommonQuery.list_schemas("mydb", extract_ownership=False)
        assert "NULL as schema_owner_name" in sql
        assert "pg_user" not in sql

    def test_list_schemas_with_ownership_joins_pg_user(self):
        sql = RedshiftCommonQuery.list_schemas("mydb", extract_ownership=True)
        assert "u.usename as schema_owner_name" in sql
        assert "LEFT JOIN pg_catalog.pg_user u ON u.usesysid = s.schema_owner" in sql

    def test_list_tables_without_ownership_uses_null_owner(self):
        sql = RedshiftCommonQuery.list_tables("mydb", extract_ownership=False)
        assert 'NULL as "owner_name"' in sql
        assert "pg_user" not in sql

    def test_list_tables_with_ownership_joins_pg_user(self):
        sql = RedshiftCommonQuery.list_tables("mydb", extract_ownership=True)
        assert 'u.usename as "owner_name"' in sql
        assert "LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner" in sql

    def test_list_tables_shared_db_without_ownership_uses_null_owner(self):
        sql = RedshiftCommonQuery.list_tables(
            "mydb", is_shared_database=True, extract_ownership=False
        )
        assert 'NULL AS "owner_name"' in sql

    def test_list_tables_shared_db_with_ownership_uses_table_owner(self):
        sql = RedshiftCommonQuery.list_tables(
            "mydb", is_shared_database=True, extract_ownership=True
        )
        assert 'table_owner AS "owner_name"' in sql
        assert "pg_user" not in sql

    def test_list_columns_late_binding_view_filters_by_view_schema(self):
        sql = RedshiftCommonQuery.list_columns("mydb", "common")
        # pg_get_late_binding_view_cols() exposes "view_schema", not "schema" —
        # the WHERE clause must use the correct column name or late-binding view
        # columns are silently dropped.
        assert "view_schema = 'common'" in sql


class TestProvisionedQueries:
    def test_list_insert_create_queries_uses_boundary_aware_listagg(self):
        sql = RedshiftProvisionedQuery.list_insert_create_queries_sql(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert PROVISIONED_LISTAGG_PATTERN in sql

    def test_temp_table_ddl_query_uses_boundary_aware_listagg(self):
        sql = RedshiftProvisionedQuery.temp_table_ddl_query(
            start_time=START_TIME, end_time=END_TIME
        )
        assert PROVISIONED_LISTAGG_PATTERN in sql

    def test_stl_scan_based_lineage_uses_boundary_aware_listagg(self):
        sql = RedshiftProvisionedQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert PROVISIONED_LISTAGG_PATTERN in sql

    def test_stl_scan_based_lineage_uses_cte_not_stl_query(self):
        """The provisioned scan lineage query should use a CTE from STL_QUERYTEXT
        instead of stl_query.querytxt (which is truncated to 4000 chars)."""
        sql = RedshiftProvisionedQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert "query_txt AS" in sql
        assert "STL_QUERYTEXT" in sql
        # Should join query_txt CTE (not stl_query table) for querytxt
        assert "join query_txt sq" in sql.lower()
        # Should NOT join stl_query table directly (only stl_querytext via CTE)
        assert "join stl_query " not in sql.lower()

    def test_no_old_listagg_pattern_provisioned(self):
        """Ensure the old LISTAGG pattern with LEN(RTRIM(text)) = 0 is gone."""
        for sql in [
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]:
            assert "LEN(RTRIM(text)) = 0" not in sql

    def test_list_all_queries_reconstructs_full_text(self):
        """Queries-v2 unified feed: all statements (reads + writes) with full text
        reconstructed from STL_QUERYTEXT, not pre-filtered by table."""
        sql = RedshiftProvisionedQuery.list_all_queries_sql()
        assert "STL_QUERYTEXT" in sql
        assert PROVISIONED_LISTAGG_PATTERN in sql
        assert "stl_query" in sql.lower()
        # Not scoped to a target/scanned table — the aggregator filters instead.
        assert "stl_scan" not in sql.lower()
        # Time window and database are bound as parameters (%s), not interpolated,
        # so config/catalog values never enter the SQL string.
        assert "sq.database = %s" in sql
        assert "sq.starttime >= %s" in sql
        assert "sq.starttime < %s" in sql
        assert sql.count("%s") == 3
        # Internal Redshift user must be excluded to avoid usage noise.
        assert "rdsdb" in sql


class TestServerlessQueries:
    def test_stl_scan_based_lineage_uses_boundary_aware_listagg(self):
        sql = RedshiftServerlessQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert SERVERLESS_LISTAGG_PATTERN_TEXT in sql

    def test_list_insert_create_queries_uses_boundary_aware_listagg(self):
        sql = RedshiftServerlessQuery.list_insert_create_queries_sql(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert SERVERLESS_LISTAGG_PATTERN_QUERYTXT in sql

    def test_temp_table_ddl_query_uses_boundary_aware_listagg(self):
        sql = RedshiftServerlessQuery.temp_table_ddl_query(
            start_time=START_TIME, end_time=END_TIME
        )
        assert SERVERLESS_LISTAGG_PATTERN_TEXT in sql

    def test_list_all_queries_reconstructs_full_text(self):
        """Queries-v2 unified feed (serverless): all statements with full text
        reconstructed from SYS_QUERY_TEXT, not pre-filtered by table."""
        sql = RedshiftServerlessQuery.list_all_queries_sql()
        assert "SYS_QUERY_HISTORY" in sql
        assert SERVERLESS_LISTAGG_PATTERN_TEXT in sql
        assert "qt.sequence < 16" in sql
        assert "SYS_QUERY_DETAIL" not in sql  # not scan/table-scoped
        # Time window and database are bound as parameters (%s), not interpolated,
        # so config/catalog values never enter the SQL string.
        assert "qh.database_name = %s" in sql
        assert "qh.start_time >= %s" in sql
        assert "qh.start_time < %s" in sql
        assert sql.count("%s") == 3
        # Internal Redshift user must be excluded to avoid usage noise.
        assert "rdsdb" in sql

    def test_no_old_listagg_pattern_serverless(self):
        """Ensure the old bare LISTAGG(qt."text") pattern is gone for serverless."""
        for sql in [
            RedshiftServerlessQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]:
            # Should not have bare LISTAGG without RTRIM wrapper
            assert 'LISTAGG(qt."text")' not in sql
            assert "LEN(RTRIM(querytxt)) = 0" not in sql


class TestQueryTextScopedToWindow:
    """The query-text sources carry no timestamp of their own (STL_QUERYTEXT has only
    userid/xid/pid/query/sequence/text), and the window predicate sits on a different
    table in the outer query -- stl_insert, stl_query or SYS_QUERY_DETAIL. So there is
    nothing for the optimizer to push into the aggregating query-text CTE: it must
    restrict its own scan to the query ids the time-filtered driving set already
    selected. Otherwise LISTAGG aggregates the cluster's whole retained query history
    on every run, and the query times out on busy clusters no matter how small
    start_time makes the window."""

    def test_provisioned_stl_scan_lineage_scopes_query_text_and_scan(self):
        sql = RedshiftProvisionedQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        # The in-window insert rows sit in a named CTE so both the STL_QUERYTEXT scan
        # and the stl_scan subquery can be scoped to it.
        assert "target_tables AS" in sql
        assert f"starttime >= '{START_TIME.strftime(redshift_datetime_format)}'" in sql
        assert_query_text_scoped(sql, "stl_querytext")
        assert_query_text_scoped(sql, "stl_scan")

    def test_provisioned_insert_create_scopes_query_text(self):
        sql = RedshiftProvisionedQuery.list_insert_create_queries_sql(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert "with target_queries as" in sql.lower()
        assert_query_text_scoped(sql, "stl_querytext")
        # stl_insert is cluster-wide, so the driving set joins SVV_TABLE_INFO to scope
        # by database too; without it the LISTAGG reassembles text for inserts into
        # every other database on the cluster, which the outer query discards.
        assert "sti.database = 'test_db'" in sql

    def test_provisioned_list_all_queries_scopes_query_text(self):
        sql = RedshiftProvisionedQuery.list_all_queries_sql()
        assert "in_window_queries" in sql
        assert_query_text_scoped(sql, "stl_querytext")
        # The window and database are bound as three positional parameters in the
        # caller's order (start_time, end_time, database); reordering them would
        # silently bind the database name into a timestamp comparison.
        assert sql.count("%s") == 3
        assert (
            sql.index("sq.starttime >= %s")
            < sql.index("sq.starttime < %s")
            < sql.index("sq.database = %s")
        )

    def test_serverless_stl_scan_lineage_scopes_query_text(self):
        sql = RedshiftServerlessQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        # SYS_QUERY_TEXT is de-duplicated with a ROW_NUMBER() window, which the outer
        # join to the time-filtered `queries` CTE cannot bound.
        assert_query_text_scoped(sql, "sys_query_text")


class TestGeneratedSqlResolves:
    """Each query's driving set lives in a CTE whose projection the outer SELECT
    depends on. A projection missing a column the outer query still reads parses
    clean and satisfies every substring assertion, failing only against a live
    cluster, so the contract needs resolving rather than matching."""

    def test_provisioned_stl_scan_based_lineage_query(self):
        assert_valid_redshift_sql(
            RedshiftProvisionedQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            expected_ctes=["target_tables", "query_txt"],
        )

    def test_provisioned_list_insert_create_queries_sql(self):
        assert_valid_redshift_sql(
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            expected_ctes=["target_queries", "query_txt"],
        )

    def test_provisioned_list_all_queries_sql(self):
        assert_valid_redshift_sql(
            RedshiftProvisionedQuery.list_all_queries_sql(),
            expected_ctes=["in_window_queries", "query_txt"],
        )

    def test_serverless_stl_scan_based_lineage_query(self):
        assert_valid_redshift_sql(
            RedshiftServerlessQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            expected_ctes=[
                "queries",
                "unique_query_text",
                "scan_queries",
                "insert_queries",
            ],
        )

    def test_serverless_list_insert_create_queries_sql(self):
        sql = RedshiftServerlessQuery.list_insert_create_queries_sql(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert_valid_redshift_sql(sql, expected_ctes=[])
        # Naming the column rather than the select alias it is given: Redshift resolves
        # an alias in WHERE, and so does qualify(), so a slip back to `cluster = ...`
        # would either empty or silently widen serverless insert lineage unnoticed.
        assert "sti.database = 'test_db'" in sql


def _assert_no_inner_join_on(sql: str, view: str) -> None:
    # The user-info views (svl_user_info / SVV_USER_INFO) enrich a log row with a
    # username; they must never gate it. Every join on them must be a LEFT join, so a
    # user the view can't resolve (rdsdb, or a real user on editions that keep these
    # views superuser/self-only) doesn't drop the row.
    lowered = sql.lower()
    view = view.lower()
    assert f"join {view}" in lowered
    assert lowered.count(f"join {view}") == lowered.count(f"left join {view}")


class TestUserInfoJoinResilience:
    """The username-resolution join to the user-info views must only enrich, never gate.
    rdsdb (the internal system user, absent from these views) is excluded explicitly by
    its system user id -- not as a side effect of an INNER join."""

    def test_provisioned_usage_query_left_joins_and_excludes_rdsdb_by_id(self):
        sql = RedshiftProvisionedQuery.usage_query(
            start_time="2024-01-01 00:00:00",
            end_time="2024-01-02 00:00:00",
            database="dev",
        )
        _assert_no_inner_join_on(sql, "svl_user_info")
        assert "sq.userid <> 1" in sql

    def test_provisioned_operation_aspect_query_left_joins_and_excludes_rdsdb(self):
        sql = RedshiftProvisionedQuery.operation_aspect_query(
            start_time="2024-01-01 00:00:00", end_time="2024-01-02 00:00:00"
        )
        # Both the insert and delete branches must LEFT join and exclude rdsdb by id.
        assert sql.lower().count("left join svl_user_info") == 2
        assert sql.count("sq.userid <> 1") == 2
        _assert_no_inner_join_on(sql, "svl_user_info")

    def test_serverless_usage_query_left_joins_and_excludes_rdsdb_by_id(self):
        sql = RedshiftServerlessQuery.usage_query(
            start_time="2024-01-01 00:00:00",
            end_time="2024-01-02 00:00:00",
            database="dev",
        )
        _assert_no_inner_join_on(sql, "svv_user_info")
        assert "qd.user_id <> 1" in sql

    def test_serverless_operation_aspect_query_left_joins_and_excludes_rdsdb(self):
        sql = RedshiftServerlessQuery.operation_aspect_query(
            start_time="2024-01-01 00:00:00", end_time="2024-01-02 00:00:00"
        )
        _assert_no_inner_join_on(sql, "svv_user_info")
        assert "qd.user_id <> 1" in sql

    def test_serverless_insert_lineage_excludes_rdsdb_by_id_not_name(self):
        # rdsdb has no row in SVV_USER_INFO, so a name-based `<> 'rdsdb'` relied on NULL
        # propagation and also dropped any real user the view couldn't resolve. Excluding
        # by user_id keeps unresolved real users (LEFT join) while still dropping rdsdb.
        sql = RedshiftServerlessQuery.list_insert_create_queries_sql(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert "qd.user_id <> 1" in sql
        assert "user_name <> 'rdsdb'" not in sql
