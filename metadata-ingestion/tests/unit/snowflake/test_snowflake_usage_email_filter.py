"""
Unit tests for Issue #15075: Column Statistics Not Generated When All Users Are Denied

These tests verify that the email filter configuration properly separates:
- Column statistics calculation (should NOT be affected by email filter)
- User information visibility (SHOULD be affected by email filter)

GitHub Issue: https://github.com/datahub-project/datahub/issues/15075
"""

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery


class TestEmailFilterSeparation:
    """
    Test that email filtering is properly separated between statistics and user information.
    """

    def test_allow_all_users_no_filter_in_query(self):
        """
        Test that allowing all users results in no email filter in the query.
        """
        email_filter = AllowDenyPattern.allow_all()
        sql_filter = SnowflakeQuery.gen_email_filter_query(email_filter)

        assert sql_filter == "", "Allow all should result in empty filter"

    def test_deny_all_users_generates_filter(self):
        """
        Test that denying all users generates the NOT rlike filter.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        sql_filter = SnowflakeQuery.gen_email_filter_query(email_filter)

        assert "AND NOT" in sql_filter, "Deny pattern should generate AND NOT clause"
        assert "rlike(user_name" in sql_filter, "Should use rlike for pattern matching"
        assert "'.*'" in sql_filter, "Should include the deny pattern"

    def test_deny_specific_pattern_generates_filter(self):
        """
        Test that denying specific patterns generates appropriate filter.
        """
        email_filter = AllowDenyPattern(deny=["test.*", "admin.*"])
        sql_filter = SnowflakeQuery.gen_email_filter_query(email_filter)

        assert "AND NOT" in sql_filter
        assert "rlike(user_name" in sql_filter
        assert "test.*" in sql_filter
        assert "admin.*" in sql_filter

    def test_allow_specific_pattern_generates_filter(self):
        """
        Test that allowing only specific patterns generates appropriate filter.
        """
        email_filter = AllowDenyPattern(allow=["analyst.*"], deny=[])
        sql_filter = SnowflakeQuery.gen_email_filter_query(email_filter)

        assert "AND" in sql_filter
        assert "rlike(user_name" in sql_filter
        assert "analyst.*" in sql_filter


class TestSeparateCTEsInQuery:
    """
    Test that the query uses separate CTEs for statistics and user information.
    """

    def test_query_has_separate_ctes_for_stats_and_users(self):
        """
        Test that the query contains both CTEs: one for stats, one for users.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Verify both CTEs exist
        assert "object_access_history_for_stats" in query, (
            "Should have CTE for statistics"
        )
        assert "object_access_history_for_users" in query, "Should have CTE for users"

    def test_stats_cte_has_no_email_filter(self):
        """
        Test that the statistics CTE does NOT contain email filtering.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Split query into lines and find the stats CTE
        lines = query.split("\n")
        in_stats_cte = False
        in_users_cte = False
        stats_cte_lines = []
        users_cte_lines = []

        for line in lines:
            if "object_access_history_for_stats AS" in line:
                in_stats_cte = True
                in_users_cte = False
            elif "object_access_history_for_users AS" in line:
                in_stats_cte = False
                in_users_cte = True
            elif "field_access_history AS" in line:
                in_stats_cte = False
                in_users_cte = False

            if in_stats_cte:
                stats_cte_lines.append(line)
            elif in_users_cte:
                users_cte_lines.append(line)

        stats_cte_text = "\n".join(stats_cte_lines)
        users_cte_text = "\n".join(users_cte_lines)

        # Stats CTE should NOT have email filter
        assert "rlike(user_name" not in stats_cte_text, (
            "Stats CTE should NOT have email filter"
        )
        assert "NVL(USERS.email" not in stats_cte_text, (
            "Stats CTE should NOT join with users table for email"
        )

        # Users CTE SHOULD have email filter
        assert "rlike(user_name" in users_cte_text, "Users CTE SHOULD have email filter"
        assert "NVL(USERS.email" in users_cte_text, (
            "Users CTE SHOULD join with users table for email"
        )

    def test_field_usage_counts_uses_stats_cte(self):
        """
        Test that field_usage_counts CTE uses the unfiltered stats CTE.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Verify field_access_history uses stats CTE
        assert "object_access_history_for_stats o" in query, (
            "field_access_history should use stats CTE"
        )

        # Verify field_usage_counts uses field_access_history (which uses stats CTE)
        lines = query.split("\n")
        in_field_usage_counts = False
        field_usage_counts_lines = []

        for line in lines:
            if "field_usage_counts AS" in line:
                in_field_usage_counts = True
            elif in_field_usage_counts and (
                "user_usage_counts AS" in line or "top_queries AS" in line
            ):
                in_field_usage_counts = False

            if in_field_usage_counts:
                field_usage_counts_lines.append(line)

        field_usage_counts_text = "\n".join(field_usage_counts_lines)
        assert "field_access_history" in field_usage_counts_text, (
            "field_usage_counts should use field_access_history"
        )

    def test_user_usage_counts_uses_users_cte(self):
        """
        Test that user_usage_counts CTE uses the filtered users CTE.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Find user_usage_counts CTE
        lines = query.split("\n")
        in_user_usage_counts = False
        user_usage_counts_lines = []

        for line in lines:
            if "user_usage_counts AS" in line:
                in_user_usage_counts = True
            elif in_user_usage_counts and "top_queries AS" in line:
                in_user_usage_counts = False

            if in_user_usage_counts:
                user_usage_counts_lines.append(line)

        user_usage_counts_text = "\n".join(user_usage_counts_lines)
        assert "object_access_history_for_users" in user_usage_counts_text, (
            "user_usage_counts should use filtered users CTE"
        )

    def test_basic_usage_counts_uses_stats_cte(self):
        """
        Test that basic_usage_counts CTE uses the unfiltered stats CTE.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Find basic_usage_counts CTE
        lines = query.split("\n")
        in_basic_usage_counts = False
        basic_usage_counts_lines = []

        for line in lines:
            if "basic_usage_counts AS" in line:
                in_basic_usage_counts = True
            elif in_basic_usage_counts and "field_usage_counts AS" in line:
                in_basic_usage_counts = False

            if in_basic_usage_counts:
                basic_usage_counts_lines.append(line)

        basic_usage_counts_text = "\n".join(basic_usage_counts_lines)
        assert "object_access_history_for_stats" in basic_usage_counts_text, (
            "basic_usage_counts should use stats CTE"
        )

    def test_top_queries_uses_stats_cte(self):
        """
        Test that top_queries CTE uses the unfiltered stats CTE.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Find top_queries CTE - look for the section between top_queries AS and the final select
        top_queries_start = query.find("top_queries AS")
        final_select_start = query.find("select\n            basic_usage_counts")

        if top_queries_start == -1 or final_select_start == -1:
            pytest.fail("Could not find top_queries CTE or final SELECT in query")

        top_queries_text = query[top_queries_start:final_select_start]

        assert "object_access_history_for_stats" in top_queries_text, (
            "top_queries should use stats CTE"
        )


class TestQueryStructureWithDifferentFilters:
    """
    Test query structure with various email filter configurations.
    """

    def test_query_structure_with_allow_all(self):
        """
        Test query structure when allowing all users.
        """
        email_filter = AllowDenyPattern.allow_all()
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Should still have separate CTEs even with allow_all
        assert "object_access_history_for_stats" in query
        assert "object_access_history_for_users" in query
        assert "field_usage_counts" in query
        assert "user_usage_counts" in query

    def test_query_structure_with_deny_all(self):
        """
        Test query structure when denying all users.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Should have separate CTEs
        assert "object_access_history_for_stats" in query
        assert "object_access_history_for_users" in query

        # Stats CTE should NOT have the deny filter
        stats_start = query.find("object_access_history_for_stats")
        stats_end = query.find("object_access_history_for_users")
        stats_section = query[stats_start:stats_end]
        assert "rlike(user_name" not in stats_section

        # Users CTE SHOULD have the deny filter
        users_start = query.find("object_access_history_for_users")
        users_end = query.find("field_access_history")
        users_section = query[users_start:users_end]
        assert "rlike(user_name" in users_section

    def test_query_structure_with_partial_deny(self):
        """
        Test query structure when denying specific user patterns.
        """
        email_filter = AllowDenyPattern(deny=["test.*", "admin.*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Should have separate CTEs
        assert "object_access_history_for_stats" in query
        assert "object_access_history_for_users" in query

        # Users CTE should have the specific deny patterns
        users_start = query.find("object_access_history_for_users")
        users_end = query.find("field_access_history")
        users_section = query[users_start:users_end]
        assert "test.*" in users_section
        assert "admin.*" in users_section


class TestBackwardCompatibility:
    """
    Test that the fix maintains backward compatibility with existing behavior.
    """

    def test_final_select_structure_unchanged(self):
        """
        Test that the final SELECT statement structure is unchanged.
        """
        email_filter = AllowDenyPattern.allow_all()
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        # Verify final SELECT has all expected columns
        assert '"OBJECT_NAME"' in query
        assert '"BUCKET_START_TIME"' in query
        assert '"OBJECT_DOMAIN"' in query
        assert '"TOTAL_QUERIES"' in query
        assert '"TOTAL_USERS"' in query
        assert '"TOP_SQL_QUERIES"' in query
        assert '"FIELD_COUNTS"' in query
        assert '"USER_COUNTS"' in query

    def test_all_required_ctes_present(self):
        """
        Test that all required CTEs are present in the query.
        """
        email_filter = AllowDenyPattern(deny=[".*"])
        query = SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            start_time_millis=1700000000000,
            end_time_millis=1700086400000,
            time_bucket_size=BucketDuration.DAY,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            email_domain=None,
            email_filter=email_filter,
        )

        required_ctes = [
            "object_access_history_for_stats",
            "object_access_history_for_users",
            "field_access_history",
            "basic_usage_counts",
            "field_usage_counts",
            "user_usage_counts",
            "top_queries",
        ]

        for cte in required_ctes:
            assert cte in query, f"Required CTE '{cte}' not found in query"
