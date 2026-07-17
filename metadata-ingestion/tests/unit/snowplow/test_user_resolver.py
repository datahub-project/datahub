"""Unit tests for UserResolver service."""

from unittest.mock import Mock

import pytest
import requests

from datahub.ingestion.source.snowplow.models.snowplow_models import User
from datahub.ingestion.source.snowplow.services.user_resolver import UserResolver


class TestUserResolverLoadUsers:
    """Tests for load_users() method."""

    def test_load_users_success(self):
        """Test successful user loading and caching."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="alice@example.com", name="Alice Smith"),
            User(id="u2", email="bob@example.com", name="Bob Jones"),
        ]
        mock_report = Mock()

        resolver = UserResolver(mock_client, mock_report)
        resolver.load_users()

        assert len(resolver._user_cache) == 2
        assert resolver._user_cache["u1"].email == "alice@example.com"
        assert resolver._user_cache["u2"].email == "bob@example.com"
        mock_report.report_warning.assert_not_called()
        mock_report.report_failure.assert_not_called()

    def test_load_users_no_client(self):
        """Test that load_users does nothing when no client is provided."""
        resolver = UserResolver(None)
        resolver.load_users()

        assert len(resolver._user_cache) == 0

    def test_load_users_api_failure_reports_warning(self):
        """Test that API failures are reported as warnings."""
        mock_client = Mock()
        mock_client.get_users.side_effect = requests.RequestException("API unavailable")
        mock_report = Mock()

        resolver = UserResolver(mock_client, mock_report)
        resolver.load_users()

        assert len(resolver._user_cache) == 0
        mock_report.report_warning.assert_called_once()
        assert "user_loading" in mock_report.report_warning.call_args[0]
        assert "API" in mock_report.report_warning.call_args[0][1]

    def test_load_users_unexpected_error_reports_failure(self):
        """Test that unexpected errors are reported as failures."""
        mock_client = Mock()
        mock_client.get_users.side_effect = RuntimeError("Unexpected error")
        mock_report = Mock()

        resolver = UserResolver(mock_client, mock_report)
        resolver.load_users()

        assert len(resolver._user_cache) == 0
        mock_report.report_failure.assert_called_once()
        assert "user_loading" in mock_report.report_failure.call_args[0]
        assert "Unexpected" in mock_report.report_failure.call_args[0][1]

    def test_load_users_caches_by_name_and_display_name(self):
        """Test that users are cached by both name and display_name."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(
                id="u1",
                email="alice@example.com",
                name="Alice Smith",
                display_name="Alice S.",
            ),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()

        # Should be in both name caches
        assert "Alice Smith" in resolver._user_name_cache
        assert "Alice S." in resolver._user_name_cache
        assert len(resolver._user_name_cache["Alice Smith"]) == 1
        assert len(resolver._user_name_cache["Alice S."]) == 1

    def test_load_users_no_report(self):
        """Test that errors are handled gracefully when no report is provided."""
        mock_client = Mock()
        mock_client.get_users.side_effect = requests.RequestException("API error")

        # Should not raise even without report
        resolver = UserResolver(mock_client, report=None)
        resolver.load_users()

        assert len(resolver._user_cache) == 0


class TestUserResolverResolveUserEmail:
    """Tests for resolve_user_email() method."""

    @pytest.fixture
    def resolver_with_users(self):
        """Create a resolver with pre-cached users."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="alice@example.com", name="Alice Smith"),
            User(id="u2", email="bob@example.com", name="Bob Jones"),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()
        return resolver

    def test_resolve_by_id_returns_email(self, resolver_with_users):
        """Test resolution by initiator ID returns email."""
        result = resolver_with_users.resolve_user_email("u1", "Wrong Name")

        assert result == "alice@example.com"

    def test_resolve_by_name_single_match(self, resolver_with_users):
        """Test resolution by name when there's a single match."""
        result = resolver_with_users.resolve_user_email(None, "Alice Smith")

        assert result == "alice@example.com"

    def test_resolve_by_name_ambiguous_returns_name(self):
        """Test that ambiguous name matches return the name directly."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="alice1@example.com", name="Alice Smith"),
            User(id="u2", email="alice2@example.com", name="Alice Smith"),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()

        result = resolver.resolve_user_email(None, "Alice Smith")

        # Should return name directly when ambiguous
        assert result == "Alice Smith"

    def test_resolve_unknown_id_falls_back_to_name(self, resolver_with_users):
        """Test that unknown ID falls back to name lookup."""
        result = resolver_with_users.resolve_user_email("unknown-id", "Alice Smith")

        assert result == "alice@example.com"

    def test_resolve_unknown_user_returns_name(self, resolver_with_users):
        """Test that completely unknown user returns the name."""
        result = resolver_with_users.resolve_user_email("unknown-id", "Unknown User")

        assert result == "Unknown User"

    def test_resolve_no_info_returns_none(self, resolver_with_users):
        """Test that no initiator info returns None."""
        result = resolver_with_users.resolve_user_email(None, None)

        assert result is None

    def test_resolve_empty_cache(self):
        """Test resolution with empty cache returns name."""
        resolver = UserResolver(None)

        result = resolver.resolve_user_email("u1", "Some Name")

        assert result == "Some Name"

    def test_id_takes_precedence_over_name(self):
        """Test that ID resolution takes precedence even if name also matches."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="by-id@example.com", name="Same Name"),
            User(id="u2", email="by-name@example.com", name="Different Name"),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()

        # ID should win even if name matches different user
        result = resolver.resolve_user_email("u1", "Different Name")
        assert result == "by-id@example.com"


class TestUserResolverCacheAccessors:
    """Tests for cache accessor methods."""

    def test_get_user_cache(self):
        """Test get_user_cache returns the cache dictionary."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="alice@example.com", name="Alice"),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()

        cache = resolver.get_user_cache()

        assert "u1" in cache
        assert cache["u1"].email == "alice@example.com"

    def test_get_user_name_cache(self):
        """Test get_user_name_cache returns the name cache dictionary."""
        mock_client = Mock()
        mock_client.get_users.return_value = [
            User(id="u1", email="alice@example.com", name="Alice"),
        ]

        resolver = UserResolver(mock_client)
        resolver.load_users()

        name_cache = resolver.get_user_name_cache()

        assert "Alice" in name_cache
        assert len(name_cache["Alice"]) == 1
