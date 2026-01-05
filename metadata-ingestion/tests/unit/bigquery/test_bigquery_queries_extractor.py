"""
Unit tests for BigQuery queries extractor user filter functionality.

This module tests the pushdown user filter feature that converts regex
patterns to BigQuery SQL WHERE clauses using REGEXP_CONTAINS.

Test Classes:
    TestBuildUserFilter: Core filter building logic
    TestEscapeForBigQueryString: SQL-injection-safe escaping (6 tests)
    TestIsAllowAllPattern: Allow-all pattern detection (8 tests)
    TestBuildEnrichedQueryLogQuery: Query builder integration (4 tests)
    TestIntegration: End-to-end flow tests
    TestFetchRegionQueryLogWithPushdown: Integration with extractor (2 tests)

Security Tests:
    - SQL injection with quote breakout attempts
    - Backslash-quote escape bypass attempts
    - Multiple backslash edge cases
    - Full integration security validation
"""

import re
from datetime import datetime, timezone

from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    _build_enriched_query_log_query,
    _build_user_filter,
    _escape_for_bigquery_string,
    _is_allow_all_pattern,
)


class TestBuildUserFilter:
    """Tests for _build_user_filter function."""

    def test_empty_patterns_returns_true(self):
        """Empty patterns should return TRUE (no filtering)."""
        result = _build_user_filter(allow_patterns=[], deny_patterns=[])
        assert result == "TRUE"

    def test_single_allow_pattern(self):
        """Single allow pattern should generate REGEXP_CONTAINS condition."""
        result = _build_user_filter(
            allow_patterns=["analyst_.*@example\\.com"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com'))"

    def test_multiple_allow_patterns(self):
        """Multiple allow patterns should be OR'd together."""
        result = _build_user_filter(
            allow_patterns=["analyst_.*@example\\.com", "admin_.*@example\\.com"],
            deny_patterns=[],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com') OR REGEXP_CONTAINS(user_email, 'admin_.*@example\\\\.com'))"
        )

    def test_single_deny_pattern(self):
        """Single deny pattern should generate NOT REGEXP_CONTAINS condition."""
        result = _build_user_filter(
            allow_patterns=[],
            deny_patterns=["bot_.*"],
        )
        assert result == "NOT REGEXP_CONTAINS(user_email, 'bot_.*')"

    def test_multiple_deny_patterns(self):
        """Multiple deny patterns should each generate separate NOT conditions."""
        result = _build_user_filter(
            allow_patterns=[],
            deny_patterns=["bot_.*", "service_.*"],
        )
        assert (
            result
            == "NOT REGEXP_CONTAINS(user_email, 'bot_.*') AND NOT REGEXP_CONTAINS(user_email, 'service_.*')"
        )

    def test_combined_allow_and_deny_patterns(self):
        """Combined allow and deny patterns should produce proper AND conditions."""
        result = _build_user_filter(
            allow_patterns=["analyst_.*@example\\.com"],
            deny_patterns=["bot_.*", "test_.*"],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, 'analyst_.*@example\\\\.com')) AND NOT REGEXP_CONTAINS(user_email, 'bot_.*') AND NOT REGEXP_CONTAINS(user_email, 'test_.*')"
        )

    def test_single_quote_escaping(self):
        """Single quotes in patterns should be escaped for SQL safety."""
        result = _build_user_filter(
            allow_patterns=["user's_pattern"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, 'user\\'s_pattern'))"

    def test_complex_regex_pattern(self):
        """Complex regex patterns should be passed through correctly."""
        result = _build_user_filter(
            allow_patterns=["^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"],
            deny_patterns=[],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$'))"
        )

    def test_wildcard_pattern(self):
        """Wildcard patterns (.*) should work correctly."""
        result = _build_user_filter(
            allow_patterns=[".*@company\\.com"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, '.*@company\\\\.com'))"

    def test_only_deny_patterns(self):
        """Only deny patterns should just have deny conditions."""
        result = _build_user_filter(
            allow_patterns=[],
            deny_patterns=["bot_.*", "service_.*"],
        )
        assert (
            result
            == "NOT REGEXP_CONTAINS(user_email, 'bot_.*') AND NOT REGEXP_CONTAINS(user_email, 'service_.*')"
        )

    # === Additional Edge Cases ===

    def test_backslash_in_pattern(self):
        """Backslashes in patterns should be doubled for SQL string literals."""
        result = _build_user_filter(
            allow_patterns=["user\\d+@example\\.com"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, 'user\\\\d+@example\\\\.com'))"

    def test_special_regex_anchors(self):
        """Regex anchors (^, $) should work correctly."""
        result = _build_user_filter(
            allow_patterns=["^admin@.*$"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, '^admin@.*$'))"

    def test_regex_groups_and_alternation(self):
        """Regex groups and alternation should be preserved."""
        result = _build_user_filter(
            allow_patterns=["(analyst|engineer)_.*@example\\.com"],
            deny_patterns=[],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, '(analyst|engineer)_.*@example\\\\.com'))"
        )

    def test_empty_string_pattern(self):
        """Empty string pattern should still work (matches empty)."""
        result = _build_user_filter(
            allow_patterns=[""],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, ''))"

    def test_dot_star_only_in_allow(self):
        """Single .* in allow should be treated as allow-all."""
        result = _build_user_filter(
            allow_patterns=[".*"],
            deny_patterns=[],
        )
        assert result == "TRUE"

    def test_multiple_patterns_including_dot_star(self):
        """Multiple allow patterns including .* should not skip filtering."""
        result = _build_user_filter(
            allow_patterns=[".*", "specific_user"],
            deny_patterns=[],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, '.*') OR REGEXP_CONTAINS(user_email, 'specific_user'))"
        )

    def test_unicode_pattern(self):
        """Unicode characters in patterns should work."""
        result = _build_user_filter(
            allow_patterns=["用户.*@example\\.com"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, '用户.*@example\\\\.com'))"

    def test_newline_in_pattern(self):
        """Newline escape sequences in patterns should be preserved."""
        result = _build_user_filter(
            allow_patterns=["user\\ntest"],
            deny_patterns=[],
        )
        assert result == "(REGEXP_CONTAINS(user_email, 'user\\\\ntest'))"

    def test_multiple_single_quotes(self):
        """Multiple single quotes should all be escaped."""
        result = _build_user_filter(
            allow_patterns=["user's_name's_pattern"],
            deny_patterns=[],
        )
        assert result.count("\\'") == 2

    def test_very_long_pattern(self):
        """Very long patterns should be handled without truncation."""
        long_domain = "a" * 100 + "@" + "b" * 100 + "\\.com"
        result = _build_user_filter(
            allow_patterns=[long_domain],
            deny_patterns=[],
        )
        expected_pattern = "a" * 100 + "@" + "b" * 100 + "\\\\.com"
        assert result == f"(REGEXP_CONTAINS(user_email, '{expected_pattern}'))"

    def test_sql_injection_attempt_semicolon(self):
        """SQL injection attempts with semicolons should be safely escaped."""
        result = _build_user_filter(
            allow_patterns=["user@example.com; DROP TABLE users;--"],
            deny_patterns=[],
        )
        # Semicolons are safe inside REGEXP_CONTAINS string literal
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, 'user@example.com; DROP TABLE users;--'))"
        )

    def test_parentheses_balanced(self):
        """Generated SQL should have balanced parentheses."""
        result = _build_user_filter(
            allow_patterns=["analyst_.*", "admin_.*"],
            deny_patterns=["bot_.*", "service_.*"],
        )
        assert result.count("(") == result.count(")")

    # === Security Tests ===

    def test_sql_injection_with_quote_breakout(self):
        """SQL injection attempts with quote breakout should be safely escaped."""
        result = _build_user_filter(
            allow_patterns=["test') OR 1=1 --"],
            deny_patterns=[],
        )
        # Quote is escaped, preventing SQL injection breakout
        assert result == "(REGEXP_CONTAINS(user_email, 'test\\') OR 1=1 --'))"

    def test_sql_injection_with_backslash_quote(self):
        """SQL injection using backslash to escape the escape should be prevented."""
        result = _build_user_filter(
            allow_patterns=["test\\"],
            deny_patterns=[],
        )
        # Backslash is doubled, preventing escape sequence injection
        assert result == "(REGEXP_CONTAINS(user_email, 'test\\\\'))"

    def test_sql_injection_backslash_before_quote(self):
        """Backslash before quote should not allow injection."""
        result = _build_user_filter(
            allow_patterns=["test\\'more"],
            deny_patterns=[],
        )
        # Both backslash and quote are escaped
        assert result == "(REGEXP_CONTAINS(user_email, 'test\\\\\\'more'))"

    def test_sql_injection_multiple_backslashes_and_quote(self):
        """Multiple backslashes followed by quote should be safely handled."""
        result = _build_user_filter(
            allow_patterns=["test\\\\'end"],
            deny_patterns=[],
        )
        # Two backslashes become four, then quote is escaped
        assert result == "(REGEXP_CONTAINS(user_email, 'test\\\\\\\\\\'end'))"

    # === Allow-All Pattern Detection Tests ===

    def test_dot_plus_treated_as_allow_all(self):
        """Pattern .+ should be treated as allow-all."""
        result = _build_user_filter(
            allow_patterns=[".+"],
            deny_patterns=[],
        )
        assert result == "TRUE"

    def test_anchored_dot_star_treated_as_allow_all(self):
        """Pattern ^.*$ should be treated as allow-all."""
        result = _build_user_filter(
            allow_patterns=["^.*$"],
            deny_patterns=[],
        )
        assert result == "TRUE"

    def test_anchored_dot_plus_treated_as_allow_all(self):
        """Pattern ^.+$ should be treated as allow-all."""
        result = _build_user_filter(
            allow_patterns=["^.+$"],
            deny_patterns=[],
        )
        assert result == "TRUE"

    def test_case_insensitive_with_flag(self):
        """Users can use (?i) flag for case-insensitive matching."""
        result = _build_user_filter(
            allow_patterns=["(?i)ANALYST_.*"],
            deny_patterns=["(?i)BOT_.*"],
        )
        assert (
            result
            == "(REGEXP_CONTAINS(user_email, '(?i)ANALYST_.*')) AND NOT REGEXP_CONTAINS(user_email, '(?i)BOT_.*')"
        )

    def test_allow_all_with_deny_patterns(self):
        """Allow-all pattern (.*) with deny patterns should still apply deny filters."""
        result = _build_user_filter(
            allow_patterns=[".*"],
            deny_patterns=["bot_.*", "service_.*"],
        )
        # Allow-all should be skipped, but deny should be applied
        assert (
            result
            == "NOT REGEXP_CONTAINS(user_email, 'bot_.*') AND NOT REGEXP_CONTAINS(user_email, 'service_.*')"
        )

    def test_service_account_pattern(self):
        """Common use case: filtering out GCP service accounts."""
        result = _build_user_filter(
            allow_patterns=[],
            deny_patterns=[".*@.*\\.iam\\.gserviceaccount\\.com"],
        )
        # Backslashes should be doubled for SQL
        assert (
            result
            == "NOT REGEXP_CONTAINS(user_email, '.*@.*\\\\.iam\\\\.gserviceaccount\\\\.com')"
        )

    def test_multiple_allow_all_patterns_with_deny(self):
        """Multiple allow-all patterns with deny should still apply deny."""
        result = _build_user_filter(
            allow_patterns=[".*", ".+"],
            deny_patterns=["bot_.*"],
        )
        # All patterns are "allow all" patterns, so only deny filter is applied
        assert result == "NOT REGEXP_CONTAINS(user_email, 'bot_.*')"


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

    def test_whitespace_nonwhitespace_star_is_allow_all(self):
        """Pattern [\\s\\S]* (whitespace + non-whitespace, zero or more) should be allow-all."""
        assert _is_allow_all_pattern(["[\\s\\S]*"]) is True

    def test_whitespace_nonwhitespace_plus_is_allow_all(self):
        """Pattern [\\s\\S]+ (whitespace + non-whitespace, one or more) should be allow-all."""
        assert _is_allow_all_pattern(["[\\s\\S]+"]) is True

    def test_specific_pattern_not_allow_all(self):
        """Specific patterns should not be allow-all."""
        assert _is_allow_all_pattern(["analyst_.*"]) is False

    def test_multiple_patterns_not_allow_all(self):
        """Multiple patterns should not be allow-all (even if one is .*)."""
        assert _is_allow_all_pattern([".*", "specific"]) is False


class TestBuildEnrichedQueryLogQuery:
    """Tests for _build_enriched_query_log_query function with user filter."""

    def test_default_user_filter(self):
        """Query should include TRUE as default user filter in WHERE clause."""
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        # Verify query structure
        assert "WHERE" in query
        assert "statement_type not in" in query
        # Verify TRUE is the user_filter (last condition before ORDER BY)
        # Using regex to precisely check the position of TRUE in the WHERE clause
        assert re.search(r"AND\s+TRUE\s+ORDER BY", query), (
            "TRUE should be the last WHERE condition before ORDER BY"
        )

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

    def test_full_flow_with_allow_deny_patterns(self):
        """Test the complete flow from patterns to SQL query."""
        user_filter = _build_user_filter(
            allow_patterns=["analyst_.*@example\\.com", "data_.*@example\\.com"],
            deny_patterns=["bot_.*", "service_account_.*"],
        )
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

    def test_full_flow_with_case_insensitive_flag(self):
        """Test full flow with (?i) flag for case insensitive matching."""
        user_filter = _build_user_filter(
            allow_patterns=["(?i)ANALYST_.*"],
            deny_patterns=["(?i)BOT_.*"],
        )
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-eu",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # Case insensitive flag should be preserved
        assert "(?i)ANALYST_.*" in query
        assert "(?i)BOT_.*" in query

    def test_no_filter_produces_valid_query(self):
        """Test that no filter (empty patterns) produces a valid query."""
        user_filter = _build_user_filter(
            allow_patterns=[],
            deny_patterns=[],
        )
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
        user_filter = _build_user_filter(
            allow_patterns=["legit@example.com"],
            deny_patterns=["attacker') OR 1=1 --"],
        )
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
    """Tests for fetch_region_query_log with pushdown username filters.

    These tests cover the integration path where pushdown_deny_usernames
    and pushdown_allow_usernames trigger the call to _build_user_filter.
    """

    def test_pushdown_with_patterns_builds_filter_and_logs(self):
        """Test that pushdown patterns build filter and log debug message."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with pushdown patterns
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_deny_usernames = ["bot_.*"]
        config.pushdown_allow_usernames = ["analyst_.*@example\\.com"]
        config.window = MagicMock()
        config.window.start_time = None
        config.window.end_time = None

        # Create mock dependencies
        mock_connection = MagicMock()
        mock_connection.query.return_value = iter([])  # Empty result set
        mock_report = MagicMock()

        # Patch the logger and __init__ to verify debug message
        logger_patch = patch(
            "datahub.ingestion.source.bigquery_v2.queries_extractor.logger"
        )
        init_patch = patch.object(
            BigQueryQueriesExtractor, "__init__", lambda self, **kwargs: None
        )

        with logger_patch as mock_logger, init_patch:
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

            # Verify _build_user_filter was called (via the query)
            # The query should contain our filter patterns
            call_args = mock_connection.query.call_args[0][0]
            assert "analyst_.*@example\\\\.com" in call_args
            assert "NOT REGEXP_CONTAINS" in call_args
            assert "bot_.*" in call_args

            # Verify debug log was called
            mock_logger.debug.assert_called()
            debug_call = str(mock_logger.debug.call_args)
            assert "pushdown user filter" in debug_call.lower()

    def test_empty_pushdown_patterns_uses_true_filter(self):
        """Test that empty pushdown patterns use TRUE as filter."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with NO pushdown patterns
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_deny_usernames = []
        config.pushdown_allow_usernames = []

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
            # When no patterns, should use TRUE
            assert "TRUE" in call_args

    def test_only_deny_patterns_builds_filter(self):
        """Test that only deny patterns (no allow) builds correct filter."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with ONLY deny patterns
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_deny_usernames = ["bot_.*", "service_.*"]
        config.pushdown_allow_usernames = []

        mock_connection = MagicMock()
        mock_connection.query.return_value = iter([])
        mock_report = MagicMock()

        with patch.object(
            BigQueryQueriesExtractor, "__init__", lambda self, **kwargs: None
        ):
            extractor = BigQueryQueriesExtractor.__new__(BigQueryQueriesExtractor)
            extractor.config = config
            extractor.connection = mock_connection
            extractor.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            extractor.end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
            extractor.structured_report = mock_report

            mock_project = MagicMock()
            mock_project.id = "test-project"

            list(extractor.fetch_region_query_log(mock_project, "region-us"))

            call_args = mock_connection.query.call_args[0][0]
            assert "NOT REGEXP_CONTAINS(user_email, 'bot_.*')" in call_args
            assert "NOT REGEXP_CONTAINS(user_email, 'service_.*')" in call_args

    def test_only_allow_patterns_builds_filter(self):
        """Test that only allow patterns (no deny) builds correct filter."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with ONLY allow patterns
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_deny_usernames = []
        config.pushdown_allow_usernames = ["analyst_.*@company\\.com"]

        mock_connection = MagicMock()
        mock_connection.query.return_value = iter([])
        mock_report = MagicMock()

        with patch.object(
            BigQueryQueriesExtractor, "__init__", lambda self, **kwargs: None
        ):
            extractor = BigQueryQueriesExtractor.__new__(BigQueryQueriesExtractor)
            extractor.config = config
            extractor.connection = mock_connection
            extractor.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            extractor.end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
            extractor.structured_report = mock_report

            mock_project = MagicMock()
            mock_project.id = "test-project"

            list(extractor.fetch_region_query_log(mock_project, "region-us"))

            call_args = mock_connection.query.call_args[0][0]
            assert (
                "REGEXP_CONTAINS(user_email, 'analyst_.*@company\\\\.com')" in call_args
            )
            assert "NOT REGEXP_CONTAINS" not in call_args


class TestBigQueryConfigValidator:
    """Tests for BigQuery config validation."""

    def test_pushdown_config_requires_queries_v2(self):
        """Test that pushdown config raises error when use_queries_v2 is False."""
        import pytest

        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        with pytest.raises(ValueError) as exc_info:
            BigQueryV2Config(
                use_queries_v2=False,
                pushdown_deny_usernames=["bot_.*"],
            )

        assert "use_queries_v2=True" in str(exc_info.value)

    def test_pushdown_config_works_with_queries_v2_enabled(self):
        """Test that pushdown config works when use_queries_v2 is True (default)."""
        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        # Should not raise - use_queries_v2 defaults to True
        config = BigQueryV2Config(
            pushdown_deny_usernames=["bot_.*"],
            pushdown_allow_usernames=["analyst_.*"],
        )
        assert config.pushdown_deny_usernames == ["bot_.*"]
        assert config.pushdown_allow_usernames == ["analyst_.*"]

    def test_empty_pushdown_config_is_valid(self):
        """Test that empty pushdown config is valid."""
        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        # Should not raise
        config = BigQueryV2Config(
            pushdown_deny_usernames=[],
            pushdown_allow_usernames=[],
        )
        assert config.pushdown_deny_usernames == []
        assert config.pushdown_allow_usernames == []

    def test_pushdown_patterns_strips_whitespace(self):
        """Test that whitespace is stripped from pushdown patterns."""
        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        config = BigQueryV2Config(
            pushdown_deny_usernames=["  bot_.*  ", " service_.*"],
            pushdown_allow_usernames=["analyst_.*  "],
        )
        # Whitespace should be stripped
        assert config.pushdown_deny_usernames == ["bot_.*", "service_.*"]
        assert config.pushdown_allow_usernames == ["analyst_.*"]

    def test_pushdown_rejects_empty_string_pattern(self):
        """Test that empty string patterns are rejected."""
        import pytest

        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        with pytest.raises(ValueError) as exc_info:
            BigQueryV2Config(
                pushdown_deny_usernames=["bot_.*", ""],
            )
        assert "Empty pattern" in str(exc_info.value)

    def test_pushdown_rejects_whitespace_only_pattern(self):
        """Test that whitespace-only patterns are rejected."""
        import pytest

        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        with pytest.raises(ValueError) as exc_info:
            BigQueryV2Config(
                pushdown_allow_usernames=["analyst_.*", "   "],
            )
        assert "Empty pattern" in str(exc_info.value)
