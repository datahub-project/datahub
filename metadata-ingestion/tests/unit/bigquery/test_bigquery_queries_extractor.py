"""
Unit tests for BigQuery queries extractor user filter functionality.

This module tests the pushdown_user_filter feature that converts Python regex
patterns (AllowDenyPattern) to BigQuery SQL WHERE clauses using REGEXP_CONTAINS.

Test Classes:
    TestBuildUserFilterFromPattern: Core filter building logic (27 tests)
    TestEscapeForBigQueryString: SQL-injection-safe escaping (6 tests)
    TestIsAllowAllPattern: Allow-all pattern detection (6 tests)
    TestBuildEnrichedQueryLogQuery: Query builder integration (4 tests)
    TestIntegration: End-to-end flow tests (4 tests)
    TestFetchRegionQueryLogWithPushdown: Integration with extractor (2 tests)

Security Tests:
    - SQL injection with quote breakout attempts
    - Backslash-quote escape bypass attempts
    - Multiple backslash edge cases
    - Full integration security validation
"""

from datetime import datetime, timezone

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    _build_enriched_query_log_query,
    _build_user_filter_from_pattern,
    _escape_for_bigquery_string,
    _is_allow_all_pattern,
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
        # Uses regular strings (not raw) with escaped backslashes
        assert "REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com')" in result

    def test_multiple_allow_patterns(self):
        """Multiple allow patterns should be OR'd together."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com", "admin_.*@example\\.com"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com')" in result
        assert "REGEXP_CONTAINS(user_email, 'admin_.*@example\\\\.com')" in result
        assert " OR " in result

    def test_single_deny_pattern(self):
        """Single deny pattern should generate NOT REGEXP_CONTAINS condition."""
        pattern = AllowDenyPattern(allow=[".*"], deny=["bot_.*"])
        result = _build_user_filter_from_pattern(pattern)
        assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in result

    def test_multiple_deny_patterns(self):
        """Multiple deny patterns should each generate separate NOT conditions."""
        pattern = AllowDenyPattern(allow=[".*"], deny=["bot_.*", "service_.*"])
        result = _build_user_filter_from_pattern(pattern)
        assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, 'service_.*')" in result
        # Both deny conditions should be ANDed
        assert result.count("NOT REGEXP_CONTAINS") == 2

    def test_combined_allow_and_deny_patterns(self):
        """Combined allow and deny patterns should produce proper AND conditions."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com"], deny=["bot_.*", "test_.*"]
        )
        result = _build_user_filter_from_pattern(pattern)
        # Should have allow condition
        assert "REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com')" in result
        # Should have deny conditions
        assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, 'test_.*')" in result
        # All conditions should be ANDed
        assert " AND " in result

    def test_case_insensitive_pattern(self):
        """Case insensitive pattern should use (?i) flag to preserve character classes."""
        pattern = AllowDenyPattern(
            allow=["ANALYST_.*"], deny=["BOT_.*"], ignoreCase=True
        )
        result = _build_user_filter_from_pattern(pattern)
        # Should use (?i) flag instead of LOWER() to preserve character class semantics
        assert "REGEXP_CONTAINS(user_email, '(?i)ANALYST_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, '(?i)BOT_.*')" in result

    def test_case_sensitive_pattern(self):
        """Case sensitive pattern (default) should not use (?i) flag."""
        pattern = AllowDenyPattern(
            allow=["Analyst_.*"], deny=["Bot_.*"], ignoreCase=False
        )
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, 'Analyst_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, 'Bot_.*')" in result
        assert "(?i)" not in result

    def test_single_quote_escaping(self):
        """Single quotes in patterns should be escaped for SQL safety."""
        pattern = AllowDenyPattern(allow=["user's_pattern"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Single quote should be escaped with backslash
        assert "\\'" in result
        # Verify it's properly escaped in the REGEXP_CONTAINS call
        assert "REGEXP_CONTAINS(user_email, 'user\\'s_pattern')" in result

    def test_complex_regex_pattern(self):
        """Complex regex patterns should be passed through correctly."""
        pattern = AllowDenyPattern(
            allow=["^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        # Backslashes are doubled for SQL string literal
        assert (
            "REGEXP_CONTAINS(user_email, '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$')"
            in result
        )

    def test_wildcard_pattern(self):
        """Wildcard patterns (.*) should work correctly."""
        pattern = AllowDenyPattern(allow=[".*@company\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert "REGEXP_CONTAINS(user_email, '.*@company\\\\.com')" in result

    def test_only_deny_with_default_allow(self):
        """Only deny patterns with default allow should just have deny conditions."""
        pattern = AllowDenyPattern.allow_all()
        pattern.deny = ["bot_.*", "service_.*"]
        result = _build_user_filter_from_pattern(pattern)
        # Should not have allow condition (default .* is skipped)
        assert result.count("REGEXP_CONTAINS") == 2  # Only deny conditions
        assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in result
        assert "NOT REGEXP_CONTAINS(user_email, 'service_.*')" in result

    # === Additional Edge Cases ===

    def test_backslash_in_pattern(self):
        """Backslashes in patterns should be doubled for SQL string literals."""
        pattern = AllowDenyPattern(allow=["user\\d+@example\\.com"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Backslash is doubled in SQL string literal
        assert "user\\\\d+@example\\\\.com" in result

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
        # Backslashes doubled for SQL
        assert "(analyst|engineer)_.*@example\\\\.com" in result

    def test_empty_string_pattern(self):
        """Empty string pattern should still work (matches empty)."""
        pattern = AllowDenyPattern(allow=[""], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Empty string is a valid regex pattern - uses regular string
        assert "REGEXP_CONTAINS(user_email, '')" in result

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
        # Backslashes doubled
        assert "用户.*@example\\\\.com" in result

    def test_newline_in_pattern(self):
        """Newline escape sequences in patterns should be preserved."""
        pattern = AllowDenyPattern(allow=["user\\ntest"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Backslashes doubled for SQL
        assert "user\\\\ntest" in result

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
        assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in result

    def test_case_insensitive_preserves_character_classes(self):
        """Case insensitive mode should preserve character classes like [A-Z]."""
        pattern = AllowDenyPattern(allow=["USER_[A-Z]+@EXAMPLE\\.COM"], ignoreCase=True)
        result = _build_user_filter_from_pattern(pattern)
        # Should use (?i) flag and preserve original pattern including [A-Z]
        # Backslashes doubled for SQL
        assert "(?i)USER_[A-Z]+@EXAMPLE\\\\.COM" in result
        # Should NOT use LOWER() which would break character classes
        assert "LOWER" not in result

    def test_very_long_pattern(self):
        """Very long patterns should be handled without truncation."""
        long_domain = "a" * 100 + "@" + "b" * 100 + "\\.com"
        pattern = AllowDenyPattern(allow=[long_domain], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Backslash in \\.com is doubled
        expected = "a" * 100 + "@" + "b" * 100 + "\\\\.com"
        assert expected in result

    def test_sql_injection_attempt_semicolon(self):
        """SQL injection attempts with semicolons should be safely escaped."""
        pattern = AllowDenyPattern(
            allow=["user@example.com; DROP TABLE users;--"], deny=[]
        )
        result = _build_user_filter_from_pattern(pattern)
        # The pattern is safely escaped in a regular string
        assert "user@example.com; DROP TABLE users;--" in result
        # Verify it's inside REGEXP_CONTAINS with regular string
        assert "REGEXP_CONTAINS(user_email, '" in result

    def test_parentheses_balanced(self):
        """Generated SQL should have balanced parentheses."""
        pattern = AllowDenyPattern(
            allow=["analyst_.*", "admin_.*"], deny=["bot_.*", "service_.*"]
        )
        result = _build_user_filter_from_pattern(pattern)
        # Count parentheses
        assert result.count("(") == result.count(")")

    # === Security Tests ===

    def test_sql_injection_with_quote_breakout(self):
        """SQL injection attempts with quote breakout should be safely escaped."""
        # This is the critical security test - the pattern tries to break out
        # of the string literal using a single quote
        pattern = AllowDenyPattern(allow=["test') OR 1=1 --"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # The single quote should be escaped
        assert "\\'" in result
        # The full pattern should be safely contained within the string
        assert "REGEXP_CONTAINS(user_email, 'test\\') OR 1=1 --')" in result

    def test_sql_injection_with_backslash_quote(self):
        """SQL injection using backslash to escape the escape should be prevented."""
        # Attack: pattern ends with backslash, trying to escape our escape character
        # e.g., input "test\" would try to make \' become \\ followed by unescaped '
        pattern = AllowDenyPattern(allow=["test\\"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Backslash should be doubled, so result is test\\ which is safe
        assert "test\\\\" in result
        # Verify the string is properly closed within REGEXP_CONTAINS
        assert "'test\\\\')" in result

    def test_sql_injection_backslash_before_quote(self):
        """Backslash before quote should not allow injection."""
        # Attack: \' in pattern - if we only escape quotes, this becomes \\' in SQL
        # which is a literal backslash followed by an unescaped quote = injection!
        pattern = AllowDenyPattern(allow=["test\\'more"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Both backslash and quote are escaped: test\\'more -> test\\\\'more in SQL
        assert "test\\\\\\'more" in result

    def test_sql_injection_multiple_backslashes_and_quote(self):
        """Multiple backslashes followed by quote should be safely handled."""
        pattern = AllowDenyPattern(allow=["test\\\\'end"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        # Each backslash doubled, then quote escaped
        # Input: test\\'end (two backslashes, then quote)
        # Output: test\\\\\\'end (four backslashes, escaped quote)
        assert "\\\\\\\\\\'" in result

    # === Allow-All Pattern Detection Tests ===

    def test_dot_plus_treated_as_allow_all(self):
        """Pattern .+ should be treated as allow-all."""
        pattern = AllowDenyPattern(allow=[".+"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert result == "TRUE"

    def test_anchored_dot_star_treated_as_allow_all(self):
        """Pattern ^.*$ should be treated as allow-all."""
        pattern = AllowDenyPattern(allow=["^.*$"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert result == "TRUE"

    def test_anchored_dot_plus_treated_as_allow_all(self):
        """Pattern ^.+$ should be treated as allow-all."""
        pattern = AllowDenyPattern(allow=["^.+$"], deny=[])
        result = _build_user_filter_from_pattern(pattern)
        assert result == "TRUE"


class TestEscapeForBigQueryString:
    """Tests for the _escape_for_bigquery_string helper function."""

    def test_no_escaping_needed(self):
        """Patterns without special chars should pass through unchanged."""
        assert _escape_for_bigquery_string("simple_pattern") == "simple_pattern"

    def test_single_quote_escaped(self):
        """Single quotes should be escaped."""
        assert _escape_for_bigquery_string("user's") == "user\\'s"

    def test_backslash_escaped(self):
        """Backslashes should be doubled."""
        assert _escape_for_bigquery_string("user\\d") == "user\\\\d"

    def test_backslash_before_quote(self):
        """Backslash before quote should both be escaped."""
        # Input: \' (backslash, quote)
        # Output: \\' (escaped backslash, escaped quote)
        assert _escape_for_bigquery_string("\\'") == "\\\\\\'"

    def test_multiple_backslashes(self):
        """Multiple backslashes should all be doubled."""
        assert _escape_for_bigquery_string("a\\\\b") == "a\\\\\\\\b"

    def test_order_matters_backslash_first(self):
        """Escaping order: backslashes first, then quotes."""
        # This is critical for security
        # Input: \' -> After backslash escape: \\' -> After quote escape: \\'
        result = _escape_for_bigquery_string("\\'")
        assert result == "\\\\\\'"


class TestIsAllowAllPattern:
    """Tests for the _is_allow_all_pattern helper function."""

    def test_empty_list_is_allow_all(self):
        """Empty pattern list should be allow-all."""
        assert _is_allow_all_pattern([]) is True

    def test_dot_star_is_allow_all(self):
        """Pattern .* should be allow-all."""
        assert _is_allow_all_pattern([".*"]) is True

    def test_dot_plus_is_allow_all(self):
        """Pattern .+ should be allow-all."""
        assert _is_allow_all_pattern([".+"]) is True

    def test_anchored_patterns_are_allow_all(self):
        """Anchored patterns ^.*$ and ^.+$ should be allow-all."""
        assert _is_allow_all_pattern(["^.*$"]) is True
        assert _is_allow_all_pattern(["^.+$"]) is True

    def test_specific_pattern_not_allow_all(self):
        """Specific patterns should not be allow-all."""
        assert _is_allow_all_pattern(["analyst_.*"]) is False

    def test_multiple_patterns_not_allow_all(self):
        """Multiple patterns should not be allow-all (even if one is .*)."""
        assert _is_allow_all_pattern([".*", "specific"]) is False


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
        user_filter = "REGEXP_CONTAINS(user_email, 'analyst_.*')"
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
        user_filter = "(REGEXP_CONTAINS(user_email, 'analyst_.*') OR REGEXP_CONTAINS(user_email, 'admin_.*')) AND NOT REGEXP_CONTAINS(user_email, 'bot_.*')"
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
            user_filter="NOT REGEXP_CONTAINS(user_email, 'service_.*')",
        )
        # Check expected columns are selected
        assert "job_id" in query
        assert "project_id" in query
        assert "user_email" in query
        assert "query" in query
        # Check table reference
        assert "`my-project`.`region-us`.INFORMATION_SCHEMA.JOBS" in query
        # Check the user filter is included
        assert "NOT REGEXP_CONTAINS(user_email, 'service_.*')" in query


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

        # Verify the filter is properly constructed (backslashes doubled)
        assert "analyst_.*@example\\\\.com" in query
        assert "data_.*@example\\\\.com" in query
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

    def test_security_sql_injection_in_full_flow(self):
        """Test that SQL injection attempts are safely handled in full flow."""
        # Critical security test: pattern with quote breakout attempt
        pattern = AllowDenyPattern(
            allow=["legit@example.com"],
            deny=["attacker') OR 1=1 --"],
        )

        user_filter = _build_user_filter_from_pattern(pattern)
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # The malicious pattern should be safely escaped with \'
        assert "attacker\\') OR 1=1 --" in query
        # The pattern should be fully contained within REGEXP_CONTAINS
        assert "NOT REGEXP_CONTAINS(user_email, 'attacker\\') OR 1=1 --')" in query


class TestFetchRegionQueryLogWithPushdown:
    """Tests for fetch_region_query_log with pushdown_user_filter enabled.

    These tests cover the integration path where pushdown_user_filter=True
    triggers the call to _build_user_filter_from_pattern.
    """

    def test_pushdown_user_filter_builds_filter_and_logs(self):
        """Test that pushdown_user_filter=True builds filter and logs debug message."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with pushdown enabled
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_user_filter = True
        config.user_email_pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com"], deny=["bot_.*"]
        )
        config.window = MagicMock()
        config.window.start_time = None
        config.window.end_time = None

        # Create mock dependencies
        mock_connection = MagicMock()
        mock_connection.query.return_value = iter([])  # Empty result set
        mock_report = MagicMock()

        # Patch the logger to verify debug message
        with (
            patch(
                "datahub.ingestion.source.bigquery_v2.queries_extractor.logger"
            ) as mock_logger,
            patch.object(
                BigQueryQueriesExtractor, "__init__", lambda self, **kwargs: None
            ),
        ):
            extractor = BigQueryQueriesExtractor.__new__(BigQueryQueriesExtractor)
            extractor.config = config
            extractor.connection = mock_connection
            extractor.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            extractor.end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
            extractor.structured_report = mock_report

            # Create mock project
            mock_project = MagicMock()
            mock_project.id = "test-project"

            # Call fetch_region_query_log which should use pushdown filter
            list(extractor.fetch_region_query_log(mock_project, "region-us"))

            # Verify _build_user_filter_from_pattern was called (via the query)
            # The query should contain our filter pattern
            call_args = mock_connection.query.call_args[0][0]
            assert "analyst_.*@example\\\\.com" in call_args
            assert "NOT REGEXP_CONTAINS" in call_args
            assert "bot_.*" in call_args

            # Verify debug log was called
            mock_logger.debug.assert_called()
            debug_call = str(mock_logger.debug.call_args)
            assert "pushdown user filter" in debug_call.lower()

    def test_pushdown_disabled_uses_true_filter(self):
        """Test that pushdown_user_filter=False uses TRUE as filter."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with pushdown DISABLED
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_user_filter = False
        config.user_email_pattern = AllowDenyPattern(
            allow=["analyst_.*@example\\.com"], deny=["bot_.*"]
        )

        # Create mock dependencies
        mock_connection = MagicMock()
        mock_connection.query.return_value = iter([])  # Empty result set
        mock_report = MagicMock()

        # Create extractor with mocked dependencies
        with patch.object(
            BigQueryQueriesExtractor, "__init__", lambda self, **kwargs: None
        ):
            extractor = BigQueryQueriesExtractor.__new__(BigQueryQueriesExtractor)
            extractor.config = config
            extractor.connection = mock_connection
            extractor.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            extractor.end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
            extractor.structured_report = mock_report

            # Create mock project
            mock_project = MagicMock()
            mock_project.id = "test-project"

            # Call fetch_region_query_log
            list(extractor.fetch_region_query_log(mock_project, "region-us"))

            # Verify the query uses TRUE (no pushdown filter)
            call_args = mock_connection.query.call_args[0][0]
            # Should NOT contain the pattern (pushdown disabled)
            assert "analyst_.*@example" not in call_args or "TRUE" in call_args
