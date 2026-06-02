import time_machine

from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

# Freeze time to ensure consistent test results
FROZEN_TIME = "2024-01-15 12:00:00"


class TestDremioTimestampFiltering:
    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_get_query_all_jobs_with_defaults(self):
        """Test that default timestamp filtering works with exact values"""
        query = DremioSQLQueries.get_query_all_jobs()

        # Check that exact time filtering is applied (1 day ago and now)
        assert "submitted_ts >= TIMESTAMP '2024-01-14 12:00:00.000'" in query
        assert "submitted_ts <= TIMESTAMP '2024-01-15 12:00:00.000'" in query
        assert "SYS.JOBS_RECENT" in query

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_get_query_all_jobs_cloud_with_defaults(self):
        """Test that default timestamp filtering works for cloud with exact values"""
        query = DremioSQLQueries.get_query_all_jobs_cloud()

        # Check that exact time filtering is applied (1 day ago and now)
        assert "submitted_ts >= TIMESTAMP '2024-01-14 12:00:00.000'" in query
        assert "submitted_ts <= TIMESTAMP '2024-01-15 12:00:00.000'" in query
        assert "sys.project.history.jobs" in query

    def test_get_query_all_jobs_with_custom_timestamps(self):
        """Test custom timestamp parameters"""
        start_time = "2023-01-01 00:00:00.000"
        end_time = "2023-01-31 23:59:59.999"

        query = DremioSQLQueries.get_query_all_jobs(
            start_timestamp_millis=start_time, end_timestamp_millis=end_time
        )

        # Check that exact custom timestamps are used
        assert f"submitted_ts >= TIMESTAMP '{start_time}'" in query
        assert f"submitted_ts <= TIMESTAMP '{end_time}'" in query

    def test_get_query_all_jobs_cloud_with_custom_timestamps(self):
        """Test custom timestamp parameters for cloud"""
        start_time = "2023-01-01 00:00:00.000"
        end_time = "2023-01-31 23:59:59.999"

        query = DremioSQLQueries.get_query_all_jobs_cloud(
            start_timestamp_millis=start_time, end_timestamp_millis=end_time
        )

        # Check that exact custom timestamps are used
        assert f"submitted_ts >= TIMESTAMP '{start_time}'" in query
        assert f"submitted_ts <= TIMESTAMP '{end_time}'" in query

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_default_timestamp_format(self):
        """Test that default timestamps have correct millisecond precision"""
        start_time = DremioSQLQueries._get_default_start_timestamp_millis()
        end_time = DremioSQLQueries._get_default_end_timestamp_millis()

        # Check format: YYYY-MM-DD HH:MM:SS.mmm
        assert len(start_time) == 23  # "2024-01-14 12:00:00.000" format
        assert start_time[19] == "."  # Decimal point position
        assert len(start_time.split(".")[-1]) == 3  # Millisecond precision

        assert len(end_time) == 23
        assert end_time[19] == "."
        assert len(end_time.split(".")[-1]) == 3

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_default_timestamp_values(self):
        """Test that default timestamps have expected values with frozen time"""
        start_time = DremioSQLQueries._get_default_start_timestamp_millis()
        end_time = DremioSQLQueries._get_default_end_timestamp_millis()

        # With frozen time, we can predict exact values
        # Start time should be 1 day ago: 2024-01-14 12:00:00.000
        assert start_time == "2024-01-14 12:00:00.000"

        # End time should be now: 2024-01-15 12:00:00.000
        assert end_time == "2024-01-15 12:00:00.000"

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_partial_timestamp_specification(self):
        """Test behavior when only one timestamp is specified"""
        start_time = "2023-01-01 00:00:00.000"

        # Only start time specified
        query = DremioSQLQueries.get_query_all_jobs(start_timestamp_millis=start_time)
        assert f"submitted_ts >= TIMESTAMP '{start_time}'" in query
        # End time should use frozen time default
        assert "submitted_ts <= TIMESTAMP '2024-01-15 12:00:00.000'" in query

        # Only end time specified
        end_time = "2023-01-31 23:59:59.999"
        query = DremioSQLQueries.get_query_all_jobs(end_timestamp_millis=end_time)
        # Start time should use frozen time default (1 day ago)
        assert "submitted_ts >= TIMESTAMP '2024-01-14 12:00:00.000'" in query
        assert f"submitted_ts <= TIMESTAMP '{end_time}'" in query

    def test_query_structure_unchanged(self):
        # Pin the WHERE invariants that gate which jobs feed lineage —
        # silently dropping them changes lineage output without a test failure.
        query = DremioSQLQueries.get_query_all_jobs()

        assert "STATUS = 'COMPLETED'" in query
        assert "LENGTH(queried_datasets)>0" in query
        assert "user_name != '$dremio$'" in query
        assert "query_type not like '%INTERNAL%'" in query

        for field in (
            "job_id",
            "user_name",
            "submitted_ts",
            "query",
            "queried_datasets",
        ):
            assert field in query, f"SELECT clause missing {field!r}"

    def test_cloud_query_structure_unchanged(self):
        # On-prem invariants plus the Dremio 26.1.0+ ARRAY → string CONCAT
        # projection for `queried_datasets` (see PR #17647).
        query = DremioSQLQueries.get_query_all_jobs_cloud()

        assert "STATUS = 'COMPLETED'" in query
        assert "ARRAY_SIZE(queried_datasets)>0" in query
        assert "user_name != '$dremio$'" in query
        assert "query_type not like '%INTERNAL%'" in query
        assert (
            "CONCAT('[', ARRAY_TO_STRING(queried_datasets, ','), ']') as queried_datasets"
            in query
        )
