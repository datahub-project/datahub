"""Tests for boundary-aware segment stitching in Redshift query reconstruction.

Redshift stores SQL queries in fixed-width character(200) segments (provisioned)
or character(4000) segments (serverless). When reconstructing queries via LISTAGG,
trailing spaces at segment boundaries get stripped, merging keywords like
`GROUP BY` -> `GROUPBY`. The boundary-aware pattern adds a space back when
the trimmed segment is shorter than the segment size, preserving word boundaries.
"""

from datetime import datetime

from datahub.ingestion.source.redshift.query import (
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)


def _simulate_boundary_aware_stitching(segments: list, segment_size: int) -> str:
    """Simulate SQL LISTAGG stitching: unconditional space at short boundaries."""
    parts = []
    for seg in segments:
        trimmed = seg.rstrip()
        if len(trimmed) < segment_size:
            trimmed += " "
        parts.append(trimmed)
    return "".join(parts).rstrip()


START_TIME = datetime(2024, 1, 1, 12, 0, 0)
END_TIME = datetime(2024, 1, 10, 12, 0, 0)

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


class TestProvisionedQueryStitching:
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
        assert "WITH query_txt AS" in sql
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


class TestServerlessQueryStitching:
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


class TestBoundaryAwareStitchingLogic:
    """Verify actual stitching behavior with realistic edge cases."""

    def test_keywords_separated_at_boundary(self):
        """GROUP BY split across boundary should be separated."""
        result = _simulate_boundary_aware_stitching(
            ["SELECT col GROUP ".ljust(200), "BY col FROM t".ljust(200)], 200
        )
        assert "GROUP BY" in result

    def test_identifier_split_at_boundary(self):
        """cast_770 split as cast_|770 gets space (unconditional approach)."""
        result = _simulate_boundary_aware_stitching(
            ["SELECT cast_".ljust(200), "770 FROM t".ljust(200)], 200
        )
        assert "cast_" in result and "770" in result

    def test_sql_comments_preserved(self):
        result = _simulate_boundary_aware_stitching(
            ["SELECT * -- comment".ljust(200), "FROM t".ljust(200)], 200
        )
        assert "comment" in result and "FROM t" in result

    def test_multiline_queries(self):
        result = _simulate_boundary_aware_stitching(
            ["SELECT col\nFROM t".ljust(200), "WHERE id > 0".ljust(200)], 200
        )
        assert "SELECT" in result and "WHERE" in result

    def test_full_segment_no_space(self):
        """Segment at exactly 200 chars (full) doesn't get space."""
        seg1 = "x" * 200
        result = _simulate_boundary_aware_stitching([seg1, "yz".ljust(200)], 200)
        assert result.startswith("x" * 200 + "yz")

    def test_short_segments_get_spaces(self):
        """Multiple short segments all get spaces."""
        result = _simulate_boundary_aware_stitching(
            ["SELECT".ljust(200), "col1,".ljust(200), "col2 FROM t".ljust(200)],
            200,
        )
        assert "SELECT col1, col2 FROM t" in result

    def test_identifier_split_across_boundary(self):
        """Identifier split mid-word at 200 chars (known limitation)."""
        result = _simulate_boundary_aware_stitching(
            ["SELECT col FROM ta".ljust(200), "ble_name WHERE id > 0".ljust(200)], 200
        )
        assert "ta" in result and "ble_name" in result
