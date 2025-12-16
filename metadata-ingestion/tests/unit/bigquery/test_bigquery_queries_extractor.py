"""
Unit tests for BigQuery queries extractor user filter functionality.

Tests the _build_user_filter_from_pattern function that converts
AllowDenyPattern (Python regex) to BigQuery SQL WHERE clauses.
"""

from datetime import datetime, timezone

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    _build_enriched_query_log_query,
    _build_user_filter_from_pattern,
)


class TestBuildUserFilterFromPattern:
    """Tests for _build_user_filter_from_pattern function."""

    def test_allow_all_pattern_returns_true(self):
        """Default allow-all pattern should return TRUE (no filtering)."""
        pattern = AllowDenyPattern.allow_all()
        result = _build_user_filter_from_pattern(pattern)
        assert result == "TRUE"

    def test_empty_pattern_returns_true(self):
        """Empty pattern should return TRUE (no filtering)."""
        pattern = AllowDenyPattern(allow=[], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert result == "TRUE"

    def test_single_allow_pattern(self):
        """Single allow pattern should generate REGEXP_CONTAINS condition."""
        pattern = AllowDenyPattern(allow=["analyst_.*@example\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, r'analyst_.*@example\\.com')" in result

    def test_multiple_allow_patterns(self):
        """Multiple allow patterns should be OR'd together."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com", "admin_.*@example\\.com"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, r'analyst_.*@example\\.com')" in result
        assert "REGEXP_CONTAINS(user_email, r'admin_.*@example\\.com')" in result
        assert " OR " in result

    def test_single_deny_pattern(self):
        """Single deny pattern should generate NOT REGEXP_CONTAINS condition."""
        pattern = AllowDenyPattern(allow=[".*"], deny=["bot_.*"])
        result = _build_user_filter_from_pattern(pattern)
        assert "NOT REGEXP_CONTAINS(user_email, r'bot_.*')" in result

    def test_multiple_deny_patterns(self):
        """Multiple deny patterns should each generate separate NOT conditions."""
        pattern = AllowDenyPattern(allow=[".*"], deny=["bot_.*", "service_.*"])
        result = _build_user_filter_from_pattern(pattern)
        assert "NOT REGEXP_CONTAINS(user_email, r'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, r'service_.*')" in result
        # Both deny conditions should be ANDed
        assert result.count("NOT REGEXP_CONTAINS") == 2

    def test_combined_allow_and_deny_patterns(self):
        """Combined allow and deny patterns should produce proper AND conditions."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com"], deny=["bot_.*", "test_.*"]
        )
        result = _build_user_filter_from_pattern(pattern)
        # Should have allow condition
        assert "REGEXP_CONTAINS(user_email, r'analyst_.*@example\\.com')" in result
        # Should have deny conditions
        assert "NOT REGEXP_CONTAINS(user_email, r'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, r'test_.*')" in result
        # All conditions should be ANDed
        assert " AND " in result

    def test_case_insensitive_pattern(self):
        """Case insensitive pattern should use (?i) flag to preserve character classes."""
        pattern = AllowDenyPattern(
            allow=["ANALYST_.*"], deny=["BOT_.*"], ignoreCase=True
        )
        result = _build_user_filter_from_pattern(pattern)
        # Should use (?i) flag instead of LOWER() to preserve character class semantics
        assert "REGEXP_CONTAINS(user_email, r'(?i)ANALYST_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, r'(?i)BOT_.*')" in result

    def test_case_sensitive_pattern(self):
        """Case sensitive pattern (default) should not use (?i) flag."""
        pattern = AllowDenyPattern(
            allow=["Analyst_.*"], deny=["Bot_.*"], ignoreCase=False
        )
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, r'Analyst_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, r'Bot_.*')" in result
        assert "(?i)" not in result

    def test_single_quote_escaping(self):
        """Single quotes in patterns should be escaped for SQL safety."""
        pattern = AllowDenyPattern(allow=["user's_pattern"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Single quote should be escaped with backslash
        assert "\\'" in result

    def test_complex_regex_pattern(self):
        """Complex regex patterns should be passed through correctly."""
        pattern = AllowDenyPattern(
            allow=["^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        assert (
            "REGEXP_CONTAINS(user_email, r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$')"
            in result
        )

    def test_wildcard_pattern(self):
        """Wildcard patterns (.*) should work correctly."""
        pattern = AllowDenyPattern(allow=[".*@company\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, r'.*@company\\.com')" in result

    def test_only_deny_with_default_allow(self):
        """Only deny patterns with default allow should just have deny conditions."""
        pattern = AllowDenyPattern.allow_all()
        pattern.deny = ["bot_.*", "service_.*"]
        result = _build_user_filter_from_pattern(pattern)
        # Should not have allow condition (default .* is skipped)
        assert result.count("REGEXP_CONTAINS") == 2  # Only deny conditions
        assert "NOT REGEXP_CONTAINS(user_email, r'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, r'service_.*')" in result

    # === Additional Edge Cases ===

    def test_backslash_in_pattern(self):
        """Backslashes in patterns should be preserved for regex."""
        pattern = AllowDenyPattern(allow=["user\\d+@example\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Backslash should be preserved in raw string
        assert "user\\d+@example\\.com" in result

    def test_special_regex_anchors(self):
        """Regex anchors (^, $) should work correctly."""
        pattern = AllowDenyPattern(allow=["^admin@.*$"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "^admin@.*$" in result

    def test_regex_groups_and_alternation(self):
        """Regex groups and alternation should be preserved."""
        pattern = AllowDenyPattern(
            allow=["(analyst|engineer)_.*@example\\.com"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        assert "(analyst|engineer)_.*@example\\.com" in result

    def test_empty_string_pattern(self):
        """Empty string pattern should still work (matches empty)."""
        pattern = AllowDenyPattern(allow=[""], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Empty string is a valid regex pattern
        assert "REGEXP_CONTAINS(user_email, r'')" in result

    def test_dot_star_only_in_allow(self):
        """Single .* in allow should be treated as allow-all."""
        pattern = AllowDenyPattern(allow=[".*"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Should return TRUE since .* is allow-all
        assert result == "TRUE"

    def test_multiple_patterns_including_dot_star(self):
        """Multiple allow patterns including .* should not skip filtering."""
        pattern = AllowDenyPattern(allow=[".*", "specific_user"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # When there are multiple patterns, we shouldn't treat as allow-all
        assert "REGEXP_CONTAINS" in result

    def test_unicode_pattern(self):
        """Unicode characters in patterns should work."""
        pattern = AllowDenyPattern(allow=["用户.*@example\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "用户.*@example\\.com" in result

    def test_newline_in_pattern(self):
        """Newline characters in patterns should be handled."""
        pattern = AllowDenyPattern(allow=["user\\ntest"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "user\\ntest" in result

    def test_multiple_single_quotes(self):
        """Multiple single quotes should all be escaped."""
        pattern = AllowDenyPattern(allow=["user's_name's_pattern"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Each single quote should be escaped
        assert result.count("\\'") == 2

    def test_deny_only_without_explicit_allow(self):
        """Deny-only patterns without explicit allow should work."""
        pattern = AllowDenyPattern(deny=["bot_.*"])
        result = _build_user_filter_from_pattern(pattern)
        assert "NOT REGEXP_CONTAINS(user_email, r'bot_.*')" in result

    def test_case_insensitive_preserves_character_classes(self):
        """Case insensitive mode should preserve character classes like [A-Z]."""
        pattern = AllowDenyPattern(allow=["USER_[A-Z]+@EXAMPLE\\.COM"], ignoreCase=True)
        result = _build_user_filter_from_pattern(pattern)
        # Should use (?i) flag and preserve original pattern including [A-Z]
        assert "(?i)USER_[A-Z]+@EXAMPLE\\.COM" in result
        # Should NOT use LOWER() which would break character classes
        assert "LOWER" not in result

    def test_very_long_pattern(self):
        """Very long patterns should be handled without truncation."""
        long_domain = "a" * 100 + "@" + "b" * 100 + "\\.com"
        pattern = AllowDenyPattern(allow=[long_domain], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert long_domain in result

    def test_sql_injection_attempt_semicolon(self):
        """SQL injection attempts with semicolons should be safe in raw strings."""
        pattern = AllowDenyPattern(
            allow=["user@example.com; DROP TABLE users;--"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        # The pattern is in a raw string, so it's treated as literal regex
        assert "user@example.com; DROP TABLE users;--" in result
        # Verify it's inside REGEXP_CONTAINS
        assert "REGEXP_CONTAINS(user_email, r'" in result

    def test_parentheses_balanced(self):
        """Generated SQL should have balanced parentheses."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*", "admin_.*"], deny=["bot_.*", "service_.*"]
        )
        result = _build_user_filter_from_pattern(pattern)
        # Count parentheses
        assert result.count("(") == result.count(")")


class TestBuildEnrichedQueryLogQuery:
    """Tests for _build_enriched_query_log_query function with user filter."""

    def test_default_user_filter(self):
        """Query should include TRUE as default user filter."""
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        # Default filter is TRUE, but it should be in the WHERE clause
        assert "TRUE" in query
        assert "WHERE" in query

    def test_custom_user_filter(self):
        """Query should include custom user filter in WHERE clause."""
        user_filter = "REGEXP_CONTAINS(user_email, r'analyst_.*')"
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )
        assert user_filter in query
        assert "WHERE" in query

    def test_complex_user_filter_in_query(self):
        """Query should properly include complex user filter."""
        user_filter = "(REGEXP_CONTAINS(user_email, r'analyst_.*') OR REGEXP_CONTAINS(user_email, r'admin_.*')) AND NOT REGEXP_CONTAINS(user_email, r'bot_.*')"
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-eu",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )
        assert user_filter in query
        # Make sure it's in the WHERE clause along with other conditions
        assert "creation_time >=" in query
        assert "error_result is null" in query

    def test_query_structure_with_filter(self):
        """Verify the overall query structure is correct with user filter."""
        query = _build_enriched_query_log_query(
            project_id="my-project",
            region="region-us",
            start_time=datetime(2024, 6, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 6, 15, tzinfo=timezone.utc),
            user_filter="NOT REGEXP_CONTAINS(user_email, r'service_.*')",
        )
        # Check expected columns are selected
        assert "job_id" in query
        assert "project_id" in query
        assert "user_email" in query
        assert "query" in query
        # Check table reference
        assert "`my-project`.`region-us`.INFORMATION_SCHEMA.JOBS" in query
        # Check the user filter is included
        assert "NOT REGEXP_CONTAINS(user_email, r'service_.*')" in query


class TestIntegration:
    """Integration tests for the full flow from pattern to query."""

    def test_full_flow_with_allow_deny_pattern(self):
        """Test the complete flow from AllowDenyPattern to SQL query."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com", "data_.*@example\\.com"],
            deny=["bot_.*", "service_account_.*"],
        )

        user_filter = _build_user_filter_from_pattern(pattern)
        query = _build_enriched_query_log_query(
            project_id="prod-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 31, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # Verify the filter is properly constructed
        assert "analyst_.*@example\\.com" in query
        assert "data_.*@example\\.com" in query
        assert "bot_.*" in query
        assert "service_account_.*" in query
        assert "NOT REGEXP_CONTAINS" in query
        assert "INFORMATION_SCHEMA.JOBS" in query

    def test_full_flow_with_case_insensitive(self):
        """Test full flow with case insensitive pattern."""
        pattern = AllowDenyPattern(
            allow=["ANALYST_.*"], deny=["BOT_.*"], ignoreCase=True
        )

        user_filter = _build_user_filter_from_pattern(pattern)
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-eu",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # Case insensitive should use (?i) flag
        assert "(?i)ANALYST_.*" in query
        assert "(?i)BOT_.*" in query
        # Should NOT use LOWER() which breaks character classes
        assert "LOWER(user_email)" not in query

    def test_no_filter_produces_valid_query(self):
        """Test that no filter (allow all) produces a valid query."""
        pattern = AllowDenyPattern.allow_all()

        user_filter = _build_user_filter_from_pattern(pattern)
        assert user_filter == "TRUE"

        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # Query should be valid with TRUE filter
        assert "TRUE" in query
        assert "INFORMATION_SCHEMA.JOBS" in query
