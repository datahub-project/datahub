"""
Unit tests for Slack feature flags.

Tests the feature flag fetching logic with various scenarios.
"""

from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.slack.feature_flags import (
    GET_SLACK_FEATURE_FLAGS_QUERY,
    _fetch_feature_flag_from_gms,
    clear_feature_flag_cache,
    get_require_slack_oauth_binding,
)


class TestFetchFeatureFlagFromGms:
    """Test GMS GraphQL fetching logic."""

    def test_fetch_true(self) -> None:
        """Test fetching flag value of True from GMS."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "appConfig": {"featureFlags": {"requireSlackOAuthBinding": True}}
        }

        result = _fetch_feature_flag_from_gms(mock_graph)

        assert result is True
        mock_graph.execute_graphql.assert_called_once_with(
            GET_SLACK_FEATURE_FLAGS_QUERY
        )

    def test_fetch_false(self) -> None:
        """Test fetching flag value of False from GMS."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "appConfig": {"featureFlags": {"requireSlackOAuthBinding": False}}
        }

        result = _fetch_feature_flag_from_gms(mock_graph)

        assert result is False
        mock_graph.execute_graphql.assert_called_once()

    def test_fetch_missing_feature_flags(self) -> None:
        """Test when featureFlags is missing from response."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"appConfig": {}}

        result = _fetch_feature_flag_from_gms(mock_graph)

        assert result is False

    def test_fetch_missing_app_config(self) -> None:
        """Test when appConfig is missing from response."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {}

        result = _fetch_feature_flag_from_gms(mock_graph)

        assert result is False

    def test_fetch_graphql_error(self) -> None:
        """Test when GraphQL query raises an error."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(Exception, match="GraphQL error"):
            _fetch_feature_flag_from_gms(mock_graph)


class TestGetRequireSlackOAuthBinding:
    """Test the main feature flag getter with caching."""

    def setup_method(self) -> None:
        """Clear cache before each test."""
        clear_feature_flag_cache()

    def teardown_method(self) -> None:
        """Clear cache after each test."""
        clear_feature_flag_cache()

    def test_fetch_from_gms_true(self) -> None:
        """Test fetching True from GMS."""
        with patch("datahub_integrations.slack.feature_flags.graph") as mock_graph:
            mock_graph.execute_graphql.return_value = {
                "appConfig": {"featureFlags": {"requireSlackOAuthBinding": True}}
            }

            result = get_require_slack_oauth_binding()

            assert result is True
            mock_graph.execute_graphql.assert_called_once()

    def test_fetch_from_gms_false(self) -> None:
        """Test fetching False from GMS."""
        with patch("datahub_integrations.slack.feature_flags.graph") as mock_graph:
            mock_graph.execute_graphql.return_value = {
                "appConfig": {"featureFlags": {"requireSlackOAuthBinding": False}}
            }

            result = get_require_slack_oauth_binding()

            assert result is False
            mock_graph.execute_graphql.assert_called_once()

    def test_gms_error_defaults_to_false(self) -> None:
        """Test that GMS errors default to False (migration mode)."""
        with patch("datahub_integrations.slack.feature_flags.graph") as mock_graph:
            mock_graph.execute_graphql.side_effect = Exception("Connection error")

            result = get_require_slack_oauth_binding()

            assert result is False
            mock_graph.execute_graphql.assert_called_once()

    def test_caching_behavior(self) -> None:
        """Test that the result is cached and GMS is only called once."""
        with patch("datahub_integrations.slack.feature_flags.graph") as mock_graph:
            mock_graph.execute_graphql.return_value = {
                "appConfig": {"featureFlags": {"requireSlackOAuthBinding": True}}
            }

            result1 = get_require_slack_oauth_binding()
            result2 = get_require_slack_oauth_binding()
            result3 = get_require_slack_oauth_binding()

            assert result1 is True
            assert result2 is True
            assert result3 is True

            mock_graph.execute_graphql.assert_called_once()

    def test_cache_clear(self) -> None:
        """Test that cache clearing forces a re-fetch."""
        with patch("datahub_integrations.slack.feature_flags.graph") as mock_graph:
            mock_graph.execute_graphql.return_value = {
                "appConfig": {"featureFlags": {"requireSlackOAuthBinding": True}}
            }

            result1 = get_require_slack_oauth_binding()
            assert result1 is True
            assert mock_graph.execute_graphql.call_count == 1

            clear_feature_flag_cache()

            result2 = get_require_slack_oauth_binding()
            assert result2 is True
            assert mock_graph.execute_graphql.call_count == 2
