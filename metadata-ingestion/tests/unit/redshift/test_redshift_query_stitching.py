"""Tests for boundary-aware segment stitching in Redshift query reconstruction."""

from datetime import datetime

from datahub.ingestion.source.redshift.query import (
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)

START_TIME = datetime(2024, 1, 1, 12, 0, 0)
END_TIME = datetime(2024, 1, 10, 12, 0, 0)


def _simulate_stitching(segments: list, segment_size: int) -> str:
    """Simulate the boundary-aware LISTAGG stitching logic in Python.

    This mirrors the SQL:
        RTRIM(LISTAGG(RTRIM(text) || CASE WHEN LEN(RTRIM(text)) < segment_size
              THEN ' ' ELSE '' END, '') WITHIN GROUP (ORDER BY sequence))
    """
    parts = []
    for seg in segments:
        trimmed = seg.rstrip()
        if len(trimmed) < segment_size:
            trimmed += " "
        parts.append(trimmed)
    return "".join(parts).rstrip()


class TestStitchingLogic:
    """Test the stitching algorithm itself, not just SQL string presence."""

    def test_space_at_boundary_preserved(self):
        # "GROUP " at end of segment 1, "BY col" at start of segment 2
        result = _simulate_stitching(
            ["SELECT col1 GROUP ".ljust(200), "BY col2 FROM t".ljust(200)],
            segment_size=200,
        )
        assert "GROUP BY" in result

    def test_no_spurious_space_when_segment_full(self):
        # Segment exactly fills 200 chars — word continues into next segment
        seg1 = "x" * 200
        seg2 = "yz FROM t".ljust(200)
        result = _simulate_stitching([seg1, seg2], segment_size=200)
        # No space inserted between segments
        assert result.startswith("x" * 200 + "yz")

    def test_single_segment(self):
        result = _simulate_stitching(["SELECT 1".ljust(200)], segment_size=200)
        assert result == "SELECT 1"

    def test_empty_segment_becomes_single_space(self):
        # All-space segment (e.g. 200 spaces) — trimmed to "", len < 200, gets " "
        result = _simulate_stitching(
            ["SELECT".ljust(200), " " * 200, "FROM t".ljust(200)],
            segment_size=200,
        )
        assert "SELECT" in result
        assert "FROM t" in result

    def test_serverless_4000_boundary(self):
        result = _simulate_stitching(
            ["SELECT col1 GROUP ".ljust(4000), "BY col2 FROM t".ljust(4000)],
            segment_size=4000,
        )
        assert "GROUP BY" in result


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
        # Time filter should appear in the relevant_queries CTE
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


class TestOldPatternRemoved:
    def test_no_old_per_segment_rtrim_pattern(self):
        """The old AWS-recommended pattern that caused the bug."""
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
