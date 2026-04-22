"""
Unit tests for BigQuery queries extractor user filter functionality.

This module tests the pushdown user filter feature that converts LIKE
patterns to BigQuery SQL WHERE clauses.

Test Classes:
    TestBuildUserFilter: Core filter building logic (24 tests)
    TestEscapeForSqlLike: SQL-injection-safe escaping (5 tests)
    TestIsAllowAllPattern: Allow-all pattern detection (5 tests)
    TestBuildEnrichedQueryLogQuery: Query builder integration (4 tests)
    TestIntegration: End-to-end flow tests (3 tests)
    TestFetchRegionQueryLogWithPushdown: Integration with extractor (4 tests)
    TestBigQueryConfigValidator: Config validation tests (6 tests)

Security Tests:
    - SQL injection with quote breakout attempts (test_sql_injection_with_quote_breakout)
    - Multiple quote escape bypass (test_sql_injection_with_multiple_quotes)
    - UNION-based SQL injection (test_sql_injection_union_attack)
    - Deny pattern injection (test_sql_injection_in_deny_pattern)
    - Full integration security validation (test_security_sql_injection_in_full_flow)
"""

import re
from datetime import datetime, timezone

from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    _build_enriched_query_log_query,
    _build_user_filter,
    _escape_for_sql_like,
    _is_allow_all_pattern,
)


class TestBuildUserFilter:
    """Tests for _build_user_filter function."""

    def test_empty_patterns_returns_true(self):
        """Empty patterns should return TRUE (no filtering)."""
        result = _build_user_filter(allow_usernames=[], deny_usernames=[])
        assert result == "TRUE"

    def test_single_allow_pattern(self):
        """Single allow pattern should generate case-insensitive LIKE condition."""
        result = _build_user_filter(
            allow_usernames=["analyst_%@example.com"],
            deny_usernames=[],
        )
        assert result == "(LOWER(user_email) LIKE 'analyst_%@example.com')"

    def test_multiple_allow_patterns(self):
        """Multiple allow patterns should be OR'd together."""
        result = _build_user_filter(
            allow_usernames=["analyst_%@example.com", "admin_%@example.com"],
            deny_usernames=[],
        )
        assert (
            result
            == "(LOWER(user_email) LIKE 'analyst_%@example.com' OR LOWER(user_email) LIKE 'admin_%@example.com')"
        )

    def test_single_deny_pattern(self):
        """Single deny pattern should generate case-insensitive NOT LIKE condition."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["bot_%"],
        )
        assert result == "LOWER(user_email) NOT LIKE 'bot_%'"

    def test_multiple_deny_patterns(self):
        """Multiple deny patterns should each generate separate NOT conditions."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["bot_%", "service_%"],
        )
        assert (
            result
            == "LOWER(user_email) NOT LIKE 'bot_%' AND LOWER(user_email) NOT LIKE 'service_%'"
        )

    def test_combined_allow_and_deny_patterns(self):
        """Combined allow and deny patterns should produce proper AND conditions."""
        result = _build_user_filter(
            allow_usernames=["analyst_%@example.com"],
            deny_usernames=["bot_%", "test_%"],
        )
        assert (
            result
            == "(LOWER(user_email) LIKE 'analyst_%@example.com') AND LOWER(user_email) NOT LIKE 'bot_%' AND LOWER(user_email) NOT LIKE 'test_%'"
        )

    def test_single_quote_escaping(self):
        """Single quotes in patterns should be escaped for SQL safety."""
        result = _build_user_filter(
            allow_usernames=["user's_pattern"],
            deny_usernames=[],
        )
        assert result == "(LOWER(user_email) LIKE 'user''s_pattern')"

    def test_wildcard_pattern(self):
        """Wildcard patterns (%) should work correctly."""
        result = _build_user_filter(
            allow_usernames=["%@company.com"],
            deny_usernames=[],
        )
        assert result == "(LOWER(user_email) LIKE '%@company.com')"

    def test_only_deny_patterns(self):
        """Only deny patterns should just have deny conditions."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["bot_%", "service_%"],
        )
        assert (
            result
            == "LOWER(user_email) NOT LIKE 'bot_%' AND LOWER(user_email) NOT LIKE 'service_%'"
        )

    def test_percent_only_in_allow(self):
        """Single % in allow should be treated as allow-all."""
        result = _build_user_filter(
            allow_usernames=["%"],
            deny_usernames=[],
        )
        assert result == "TRUE"

    def test_multiple_patterns_including_percent(self):
        """Multiple allow patterns including % should not skip filtering."""
        result = _build_user_filter(
            allow_usernames=["%", "specific_user"],
            deny_usernames=[],
        )
        assert (
            result
            == "(LOWER(user_email) LIKE '%' OR LOWER(user_email) LIKE 'specific_user')"
        )

    def test_unicode_pattern(self):
        """Unicode characters in patterns should work."""
        result = _build_user_filter(
            allow_usernames=["用户%@example.com"],
            deny_usernames=[],
        )
        assert result == "(LOWER(user_email) LIKE '用户%@example.com')"

    def test_multiple_single_quotes(self):
        """Multiple single quotes should all be escaped."""
        result = _build_user_filter(
            allow_usernames=["user's_name's_pattern"],
            deny_usernames=[],
        )
        assert result.count("''") == 2

    def test_very_long_pattern(self):
        """Very long patterns should be handled without truncation."""
        long_domain = "a" * 100 + "@" + "b" * 100 + ".com"
        result = _build_user_filter(
            allow_usernames=[long_domain],
            deny_usernames=[],
        )
        assert result == f"(LOWER(user_email) LIKE '{long_domain}')"

    def test_sql_injection_attempt_semicolon(self):
        """SQL injection attempts with semicolons should be safely escaped."""
        result = _build_user_filter(
            allow_usernames=["user@example.com; DROP TABLE users;--"],
            deny_usernames=[],
        )
        # Semicolons are safe inside LIKE string literal, pattern is lowercased
        assert (
            result == "(LOWER(user_email) LIKE 'user@example.com; drop table users;--')"
        )

    def test_parentheses_balanced(self):
        """Generated SQL should have balanced parentheses."""
        result = _build_user_filter(
            allow_usernames=["analyst_%", "admin_%"],
            deny_usernames=["bot_%", "service_%"],
        )
        assert result.count("(") == result.count(")")

    # === Security Tests ===

    def test_sql_injection_with_quote_breakout(self):
        """SQL injection attempts with quote breakout should be safely escaped."""
        result = _build_user_filter(
            allow_usernames=["test' OR 1=1 --"],
            deny_usernames=[],
        )
        # Quote is escaped by doubling, preventing SQL injection breakout, pattern is lowercased
        assert result == "(LOWER(user_email) LIKE 'test'' or 1=1 --')"

    def test_sql_injection_with_multiple_quotes(self):
        """Multiple quote injection attempts should all be escaped."""
        result = _build_user_filter(
            allow_usernames=["test''' OR ''='"],
            deny_usernames=[],
        )
        # Each quote should be doubled: 3 quotes → 6, 2 quotes → 4, 1 quote → 2, pattern is lowercased
        assert result == "(LOWER(user_email) LIKE 'test'''''' or ''''=''')"

    def test_sql_injection_union_attack(self):
        """UNION-based SQL injection should be safely contained in string."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["' UNION SELECT * FROM users --"],
        )
        # The entire attack is contained within the LIKE string, pattern is lowercased
        assert result == "LOWER(user_email) NOT LIKE ''' union select * from users --'"

    def test_sql_injection_in_deny_pattern(self):
        """SQL injection in deny pattern should be safely escaped."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["admin'--"],
        )
        assert result == "LOWER(user_email) NOT LIKE 'admin''--'"

    # === Allow-All Pattern Detection Tests ===

    def test_allow_all_with_deny_patterns(self):
        """Allow-all pattern (%) with deny patterns should still apply deny filters."""
        result = _build_user_filter(
            allow_usernames=["%"],
            deny_usernames=["bot_%", "service_%"],
        )
        # Allow-all should be skipped, but deny should be applied
        assert (
            result
            == "LOWER(user_email) NOT LIKE 'bot_%' AND LOWER(user_email) NOT LIKE 'service_%'"
        )

    def test_service_account_pattern(self):
        """Common use case: filtering out GCP service accounts."""
        result = _build_user_filter(
            allow_usernames=[],
            deny_usernames=["%@%.iam.gserviceaccount.com"],
        )
        assert result == "LOWER(user_email) NOT LIKE '%@%.iam.gserviceaccount.com'"

    def test_multiple_allow_all_patterns_with_deny(self):
        """Multiple allow-all patterns with deny should still apply deny."""
        result = _build_user_filter(
            allow_usernames=["%", "%"],
            deny_usernames=["bot_%"],
        )
        # All patterns are "allow all" patterns, so only deny filter is applied
        assert result == "LOWER(user_email) NOT LIKE 'bot_%'"


class TestEscapeForSqlLike:
    """Tests for the _escape_for_sql_like helper function."""

    def test_no_escaping_needed(self):
        """Patterns without special chars should pass through unchanged."""
        assert _escape_for_sql_like("simple_pattern") == "simple_pattern"

    def test_single_quote_escaped(self):
        """Single quotes should be doubled."""
        assert _escape_for_sql_like("user's") == "user''s"

    def test_multiple_single_quotes(self):
        """Multiple single quotes should all be doubled."""
        assert _escape_for_sql_like("a'b'c") == "a''b''c"

    def test_percent_preserved(self):
        """Percent signs should be preserved (they're LIKE wildcards)."""
        assert _escape_for_sql_like("%pattern%") == "%pattern%"

    def test_underscore_preserved(self):
        """Underscores should be preserved (they're LIKE single char wildcards)."""
        assert _escape_for_sql_like("user_name") == "user_name"


class TestIsAllowAllPattern:
    """Tests for the _is_allow_all_pattern helper function."""

    def test_empty_list_is_allow_all(self):
        """Empty pattern list should be allow-all."""
        assert _is_allow_all_pattern([]) is True

    def test_percent_is_allow_all(self):
        """Pattern % should be allow-all."""
        assert _is_allow_all_pattern(["%"]) is True

    def test_specific_pattern_not_allow_all(self):
        """Specific patterns should not be allow-all."""
        assert _is_allow_all_pattern(["analyst_%"]) is False

    def test_multiple_patterns_not_allow_all(self):
        """Multiple patterns should not be allow-all (even if one is %)."""
        assert _is_allow_all_pattern(["%", "specific"]) is False

    def test_multiple_percent_is_allow_all(self):
        """Multiple % patterns should be allow-all."""
        assert _is_allow_all_pattern(["%", "%"]) is True


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
        user_filter = "LOWER(user_email) LIKE 'analyst_%'"
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
        user_filter = "(LOWER(user_email) LIKE 'analyst_%' OR LOWER(user_email) LIKE 'admin_%') AND LOWER(user_email) NOT LIKE 'bot_%'"
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
            user_filter="LOWER(user_email) NOT LIKE 'service_%'",
        )
        # Check expected columns are selected
        assert "job_id" in query
        assert "project_id" in query
        assert "user_email" in query
        assert "query" in query
        # Check table reference
        assert "`my-project`.`region-us`.INFORMATION_SCHEMA.JOBS" in query
        # Check the user filter is included
        assert "LOWER(user_email) NOT LIKE 'service_%'" in query


class TestIntegration:
    """Integration tests for the full flow from pattern to query."""

    def test_full_flow_with_allow_deny_patterns(self):
        """Test the complete flow from patterns to SQL query."""
        user_filter = _build_user_filter(
            allow_usernames=["analyst_%@example.com", "data_%@example.com"],
            deny_usernames=["bot_%", "service_account_%"],
        )
        query = _build_enriched_query_log_query(
            project_id="prod-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 31, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # Verify the filter is properly constructed
        assert "analyst_%@example.com" in query
        assert "data_%@example.com" in query
        assert "bot_%" in query
        assert "service_account_%" in query
        assert "NOT LIKE" in query
        assert "INFORMATION_SCHEMA.JOBS" in query

    def test_no_filter_produces_valid_query(self):
        """Test that no filter (empty patterns) produces a valid query."""
        user_filter = _build_user_filter(
            allow_usernames=[],
            deny_usernames=[],
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
            allow_usernames=["legit@example.com"],
            deny_usernames=["attacker' OR 1=1 --"],
        )
        query = _build_enriched_query_log_query(
            project_id="test-project",
            region="region-us",
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            user_filter=user_filter,
        )

        # The malicious pattern should be safely escaped with '', and lowercased
        assert "attacker'' or 1=1 --" in query
        # The pattern should be fully contained within NOT LIKE
        assert "LOWER(user_email) NOT LIKE 'attacker'' or 1=1 --'" in query


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
        config.pushdown_deny_usernames = ["bot_%"]
        config.pushdown_allow_usernames = ["analyst_%@example.com"]
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
            # The query should contain our filter patterns (case-insensitive)
            executed_query_sql = mock_connection.query.call_args[0][0]
            assert "analyst_%@example.com" in executed_query_sql
            assert "NOT LIKE" in executed_query_sql
            assert "bot_%" in executed_query_sql
            assert "LOWER" in executed_query_sql

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
            executed_query_sql = mock_connection.query.call_args[0][0]
            # When no patterns, should use TRUE
            assert "TRUE" in executed_query_sql

    def test_only_deny_patterns_builds_filter(self):
        """Test that only deny patterns (no allow) builds correct filter."""
        from unittest.mock import MagicMock, patch

        from datahub.ingestion.source.bigquery_v2.queries_extractor import (
            BigQueryQueriesExtractor,
            BigQueryQueriesExtractorConfig,
        )

        # Create a mock config with ONLY deny patterns
        config = MagicMock(spec=BigQueryQueriesExtractorConfig)
        config.pushdown_deny_usernames = ["bot_%", "service_%"]
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

            executed_query_sql = mock_connection.query.call_args[0][0]
            assert "LOWER(user_email) NOT LIKE 'bot_%'" in executed_query_sql
            assert "LOWER(user_email) NOT LIKE 'service_%'" in executed_query_sql

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
        config.pushdown_allow_usernames = ["analyst_%@company.com"]

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

            executed_query_sql = mock_connection.query.call_args[0][0]
            assert (
                "LOWER(user_email) LIKE 'analyst_%@company.com'" in executed_query_sql
            )
            assert "NOT LIKE" not in executed_query_sql


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
                pushdown_deny_usernames=["bot_%"],
            )

        assert "use_queries_v2=True" in str(exc_info.value)

    def test_pushdown_config_works_with_queries_v2_enabled(self):
        """Test that pushdown config works when use_queries_v2 is True (default)."""
        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        # Should not raise - use_queries_v2 defaults to True
        config = BigQueryV2Config(
            pushdown_deny_usernames=["bot_%"],
            pushdown_allow_usernames=["analyst_%"],
        )
        assert config.pushdown_deny_usernames == ["bot_%"]
        assert config.pushdown_allow_usernames == ["analyst_%"]

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
            pushdown_deny_usernames=["  bot_%  ", " service_%"],
            pushdown_allow_usernames=["analyst_%  "],
        )
        # Whitespace should be stripped
        assert config.pushdown_deny_usernames == ["bot_%", "service_%"]
        assert config.pushdown_allow_usernames == ["analyst_%"]

    def test_pushdown_rejects_empty_string_pattern(self):
        """Test that empty string patterns are rejected."""
        import pytest

        from datahub.ingestion.source.bigquery_v2.bigquery_config import (
            BigQueryV2Config,
        )

        with pytest.raises(ValueError) as exc_info:
            BigQueryV2Config(
                pushdown_deny_usernames=["bot_%", ""],
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
                pushdown_allow_usernames=["analyst_%", "   "],
            )
        assert "Empty pattern" in str(exc_info.value)
