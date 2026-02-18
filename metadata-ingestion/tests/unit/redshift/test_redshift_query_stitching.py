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
