from datetime import datetime, timezone

from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    _build_enriched_query_log_query,
    _build_user_filter,
)


class TestBuildUserFilter:
    """Tests for the _build_user_filter function."""

    def test_no_filters_returns_true(self) -> None:
        """Test with no filters returns 'TRUE' (no-op)."""
        result = _build_user_filter()
        assert result == "TRUE"

    def test_empty_lists_returns_true(self) -> None:
        """Test with empty lists returns 'TRUE' (no-op)."""
        result = _build_user_filter(deny_usernames=[], allow_usernames=[])
        assert result == "TRUE"

    def test_deny_single_pattern(self) -> None:
        """Test deny filter with single pattern."""
        result = _build_user_filter(deny_usernames=["service_%@example.com"])
        assert "LOWER(user_email) NOT LIKE LOWER('service_%@example.com')" in result
        assert result != "TRUE"

    def test_deny_multiple_patterns(self) -> None:
        """Test deny filter with multiple patterns - should be AND'ed."""
        result = _build_user_filter(
            deny_usernames=["service_%@example.com", "test_%@example.com"]
        )
        assert "LOWER(user_email) NOT LIKE LOWER('service_%@example.com')" in result
        assert "LOWER(user_email) NOT LIKE LOWER('test_%@example.com')" in result
        assert " AND " in result

    def test_allow_single_pattern(self) -> None:
        """Test allow filter with single pattern."""
        result = _build_user_filter(allow_usernames=["analyst_%@example.com"])
        assert "LOWER(user_email) LIKE LOWER('analyst_%@example.com')" in result
        assert "NOT LIKE" not in result

    def test_allow_multiple_patterns(self) -> None:
        """Test allow filter with multiple patterns - should be OR'ed."""
        result = _build_user_filter(
            allow_usernames=["analyst_%@example.com", "dev_%@example.com"]
        )
        assert "LOWER(user_email) LIKE LOWER('analyst_%@example.com')" in result
        assert "LOWER(user_email) LIKE LOWER('dev_%@example.com')" in result
        assert " OR " in result

    def test_combined_deny_and_allow(self) -> None:
        """Test combined deny and allow filters."""
        result = _build_user_filter(
            deny_usernames=["service_%@example.com"],
            allow_usernames=["analyst_%@example.com"],
        )
        # Should have both conditions
        assert "NOT LIKE" in result
        assert "LIKE LOWER('analyst_%@example.com')" in result
        # Should be AND'ed together at the top level
        assert " AND " in result

    def test_sql_injection_prevention(self) -> None:
        """Test that single quotes are escaped to prevent SQL injection."""
        result = _build_user_filter(deny_usernames=["user'name@example.com"])
        # Single quote should be escaped to double single quotes
        assert "user''name@example.com" in result

    def test_case_insensitivity_with_lower(self) -> None:
        """Test that LOWER() is used for case-insensitive matching."""
        result = _build_user_filter(allow_usernames=["User@Example.COM"])
        # Should wrap both sides in LOWER() for case-insensitive matching
        assert "LOWER(user_email)" in result
        assert "LOWER('User@Example.COM')" in result

    def test_wildcard_percent_pattern(self) -> None:
        """Test SQL wildcard % pattern is preserved."""
        result = _build_user_filter(allow_usernames=["%@example.com"])
        assert "%@example.com" in result

    def test_wildcard_underscore_pattern(self) -> None:
        """Test SQL wildcard _ pattern is preserved."""
        result = _build_user_filter(allow_usernames=["user_test@example.com"])
        assert "user_test@example.com" in result

    def test_service_account_pattern(self) -> None:
        """Test typical service account pattern."""
        result = _build_user_filter(
            deny_usernames=["service_%@%.iam.gserviceaccount.com"]
        )
        assert "service_%@%.iam.gserviceaccount.com" in result
        assert "NOT LIKE" in result

    def test_multiple_deny_all_must_match(self) -> None:
        """Test that multiple deny patterns are ALL applied (AND logic)."""
        result = _build_user_filter(
            deny_usernames=["a@example.com", "b@example.com", "c@example.com"]
        )
        # Count AND occurrences - should have 2 ANDs for 3 conditions
        assert result.count(" AND ") == 2

    def test_multiple_allow_any_can_match(self) -> None:
        """Test that multiple allow patterns use OR logic (any can match)."""
        result = _build_user_filter(
            allow_usernames=["a@example.com", "b@example.com", "c@example.com"]
        )
        # Count OR occurrences - should have 2 ORs for 3 conditions
        assert result.count(" OR ") == 2

    def test_combined_multiple_deny_and_allow(self) -> None:
        """Test with multiple patterns for both deny and allow."""
        result = _build_user_filter(
            deny_usernames=["service_%", "bot_%"],
            allow_usernames=["analyst_%", "dev_%"],
        )
        # Deny conditions are AND'ed within their group
        assert "NOT LIKE LOWER('service_%')" in result
        assert "NOT LIKE LOWER('bot_%')" in result
        # Allow conditions are OR'ed within their group
        assert "LIKE LOWER('analyst_%')" in result
        assert "LIKE LOWER('dev_%')" in result
        # Both groups are AND'ed together
        # Structure: (deny1 AND deny2) AND (allow1 OR allow2)
        assert result.count(" AND ") == 2  # 1 in deny group + 1 joining groups
        assert result.count(" OR ") == 1  # 1 in allow group

    def test_none_vs_empty_list_consistent(self) -> None:
        """Test that None and empty list produce identical results."""
        result_both_none = _build_user_filter(deny_usernames=None, allow_usernames=None)
        result_both_empty = _build_user_filter(deny_usernames=[], allow_usernames=[])
        result_mixed1 = _build_user_filter(deny_usernames=None, allow_usernames=[])
        result_mixed2 = _build_user_filter(deny_usernames=[], allow_usernames=None)

        assert result_both_none == "TRUE"
        assert result_both_empty == "TRUE"
        assert result_mixed1 == "TRUE"
        assert result_mixed2 == "TRUE"

    def test_pattern_with_multiple_special_chars(self) -> None:
        """Test patterns with multiple SQL LIKE wildcards."""
        result = _build_user_filter(allow_usernames=["%_service_%@%.iam.%"])
        assert "%_service_%@%.iam.%" in result


class TestBuildEnrichedQueryLogQuery:
    """Tests for the _build_enriched_query_log_query function."""

    def test_query_without_user_filters_has_true(self) -> None:
        """Test query generation without user filters includes TRUE."""
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

        query = _build_enriched_query_log_query(
            project_id="my-project",
            region="region-us",
            start_time=start_time,
            end_time=end_time,
        )

        assert "my-project" in query
        assert "region-us" in query
        assert "INFORMATION_SCHEMA.JOBS" in query
        # Should have TRUE as the user filter (no-op)
        assert "TRUE" in query

    def test_query_with_deny_filter(self) -> None:
        """Test query generation with deny filter."""
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

        query = _build_enriched_query_log_query(
            project_id="my-project",
            region="region-us",
            start_time=start_time,
            end_time=end_time,
            deny_usernames=["service_%@example.com"],
        )

        assert "NOT LIKE LOWER('service_%@example.com')" in query

    def test_query_with_allow_filter(self) -> None:
        """Test query generation with allow filter."""
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

        query = _build_enriched_query_log_query(
            project_id="my-project",
            region="region-us",
            start_time=start_time,
            end_time=end_time,
            allow_usernames=["analyst_%@example.com"],
        )

        assert "LIKE LOWER('analyst_%@example.com')" in query
        assert "NOT LIKE" not in query

    def test_query_with_both_filters(self) -> None:
        """Test query generation with both deny and allow filters."""
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

        query = _build_enriched_query_log_query(
            project_id="my-project",
            region="region-us",
            start_time=start_time,
            end_time=end_time,
            deny_usernames=["service_%@example.com"],
            allow_usernames=["analyst_%@example.com"],
        )

        assert "NOT LIKE LOWER('service_%@example.com')" in query
        assert "LIKE LOWER('analyst_%@example.com')" in query
