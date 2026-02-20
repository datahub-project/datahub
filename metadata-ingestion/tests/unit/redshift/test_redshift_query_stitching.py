"""Tests for keyword-aware segment stitching in Redshift query reconstruction."""

from datetime import datetime

from datahub.ingestion.source.redshift.query import (
    REDSHIFT_RESERVED_KEYWORDS,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
    stitch_query_segments,
)

START_TIME = datetime(2024, 1, 1, 12, 0, 0)
END_TIME = datetime(2024, 1, 10, 12, 0, 0)

# Shorthand for the boundary marker used in tests
_B = chr(1)


class TestStitchQuerySegments:
    """Test the keyword-aware stitching function directly."""

    def test_no_markers_passthrough(self):
        assert stitch_query_segments("SELECT 1 FROM t") == "SELECT 1 FROM t"

    def test_keyword_at_boundary_gets_space(self):
        # GROUP is a reserved keyword — space must be inserted
        raw = f"SELECT col1 GROUP{_B}BY col2 FROM t"
        assert stitch_query_segments(raw) == "SELECT col1 GROUP BY col2 FROM t"

    def test_non_keyword_at_boundary_no_space(self):
        # Neither "ca" nor "st_770" is a keyword — no space inserted
        raw = f"q1.ca{_B}st_770"
        assert stitch_query_segments(raw) == "q1.cast_770"

    def test_cast_770_case(self):
        # Michael's exact counterexample: identifier split across segments
        raw = f"SELECT q1.ca{_B}st_770 FROM t"
        assert stitch_query_segments(raw) == "SELECT q1.cast_770 FROM t"

    def test_keyword_after_boundary(self):
        # AS is a keyword on the right side
        raw = f"123{_B}AS col"
        assert stitch_query_segments(raw) == "123 AS col"

    def test_keyword_before_boundary(self):
        # FROM is a keyword on the left side
        raw = f"SELECT col FROM{_B}t"
        assert stitch_query_segments(raw) == "SELECT col FROM t"

    def test_multiple_boundaries(self):
        raw = f"SELECT col1 GROUP{_B}BY col2 ORDER{_B}BY col3"
        assert stitch_query_segments(raw) == "SELECT col1 GROUP BY col2 ORDER BY col3"

    def test_trailing_marker_ignored(self):
        raw = f"SELECT 1{_B}"
        assert stitch_query_segments(raw) == "SELECT 1"

    def test_keyword_case_insensitive(self):
        # Keywords should match regardless of case in the source
        raw = f"select col from{_B}t"
        assert stitch_query_segments(raw) == "select col from t"

    def test_non_word_at_boundary(self):
        # Boundary between non-word characters — no keyword match, no space
        raw = f"1+2{_B}*3"
        assert stitch_query_segments(raw) == "1+2*3"

    def test_where_keyword(self):
        raw = f"FROM t WHERE{_B}col = 1"
        assert stitch_query_segments(raw) == "FROM t WHERE col = 1"

    def test_join_keyword(self):
        raw = f"t1 INNER{_B}JOIN t2 ON"
        assert stitch_query_segments(raw) == "t1 INNER JOIN t2 ON"

    def test_identifier_with_dollar_sign(self):
        # Dollar sign is valid in Redshift identifiers
        raw = f"my$va{_B}r_name"
        assert stitch_query_segments(raw) == "my$var_name"


class TestProvisionedScanLineageCTE:
    """The structural change: CTE from STL_QUERYTEXT instead of stl_query.querytxt."""

    def test_uses_cte_not_stl_query(self):
        sql = RedshiftProvisionedQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert "STL_QUERYTEXT" in sql
        assert "join query_txt sq" in sql.lower()
        assert "join stl_query " not in sql.lower()

    def test_cte_is_scoped_to_time_range(self):
        sql = RedshiftProvisionedQuery.stl_scan_based_lineage_query(
            db_name="test_db", start_time=START_TIME, end_time=END_TIME
        )
        assert "relevant_queries" in sql
        assert "2024-01-01 12:00:00" in sql
        assert "2024-01-10 12:00:00" in sql


class TestSegmentSizeNotCrossed:
    """Provisioned queries use 200, serverless use 4000. Never mixed."""

    def test_provisioned_uses_200_not_4000(self):
        for sql in [
            RedshiftProvisionedQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]:
            assert "< 200" in sql
            assert "< 4000" not in sql

    def test_serverless_uses_4000_not_200(self):
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
            assert "< 4000" in sql
            assert "< 200" not in sql


class TestChr1MarkerInSQL:
    """All LISTAGG locations use CHR(1) marker, not unconditional space."""

    def test_provisioned_uses_chr1(self):
        for sql in [
            RedshiftProvisionedQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]:
            assert "CHR(1)" in sql
            assert "THEN ' '" not in sql

    def test_serverless_uses_chr1(self):
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
            assert "CHR(1)" in sql
            assert "THEN ' '" not in sql


class TestOldPatternRemoved:
    def test_no_old_unconditional_space_pattern(self):
        """Old THEN ' ' pattern should be gone from all LISTAGG queries."""
        all_sqls = [
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]
        for sql in all_sqls:
            assert "LEN(RTRIM(text)) = 0" not in sql
            assert "LEN(RTRIM(querytxt)) = 0" not in sql

    def test_no_rtrim_wrapping_listagg(self):
        """RTRIM should not wrap the entire LISTAGG result."""
        all_sqls = [
            RedshiftProvisionedQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftProvisionedQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.stl_scan_based_lineage_query(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.list_insert_create_queries_sql(
                db_name="test_db", start_time=START_TIME, end_time=END_TIME
            ),
            RedshiftServerlessQuery.temp_table_ddl_query(
                start_time=START_TIME, end_time=END_TIME
            ),
        ]
        for sql in all_sqls:
            assert "RTRIM(LISTAGG" not in sql


class TestKeywordSet:
    def test_common_keywords_present(self):
        for kw in [
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP",
            "ORDER",
            "BY",
            "JOIN",
            "AS",
            "INSERT",
            "UPDATE",
            "DELETE",
            "SET",
        ]:
            assert kw in REDSHIFT_RESERVED_KEYWORDS

    def test_non_keywords_absent(self):
        for word in ["CAST_770", "MY_TABLE", "FOO", "BAR"]:
            assert word not in REDSHIFT_RESERVED_KEYWORDS
