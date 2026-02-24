"""
Unit tests for Slack user resolution and DataHub user lookup.

Tests the user resolution logic with OAuth binding and email fallback.
"""

from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import slack_bolt
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.slack.utils.datahub_user import (
    UserResolutionResult,
    _get_slack_email_from_user_id,
    _search_datahub_users_by_email,
    _search_datahub_users_by_slack_id,
    build_authorization_error_blocks,
    build_connect_account_blocks,
    get_datahub_user,
    get_datahub_user_by_slack_id,
    get_user_information,
    graph_as_user,
    resolve_slack_user_to_datahub,
)


@pytest.fixture
def mock_slack_app() -> Any:
    """Create a mock Slack app."""
    app = MagicMock(spec=slack_bolt.App)
    return app


@pytest.fixture
def mock_graph() -> Any:
    """Create a mock DataHub graph client."""
    graph = MagicMock(spec=DataHubGraph)
    return graph


class TestGetSlackEmailFromUserId:
    """Test extracting email from Slack user ID."""

    def test_get_email_success(self, mock_slack_app: Any) -> None:
        """Test successfully getting email from Slack."""
        mock_slack_app.client.users_info.return_value = {
            "ok": True,
            "user": {"profile": {"email": "john@example.com"}},
        }

        result = _get_slack_email_from_user_id(mock_slack_app, "U12345")

        assert result == "john@example.com"
        mock_slack_app.client.users_info.assert_called_once_with(user="U12345")

    def test_get_email_no_profile(self, mock_slack_app: Any) -> None:
        """Test when user has no email in profile."""
        mock_slack_app.client.users_info.return_value = {
            "ok": True,
            "user": {"profile": {}},
        }

        result = _get_slack_email_from_user_id(mock_slack_app, "U12345")

        assert result is None

    def test_get_email_api_error(self, mock_slack_app: Any) -> None:
        """Test when Slack API returns error."""
        mock_slack_app.client.users_info.return_value = {"ok": False}

        result = _get_slack_email_from_user_id(mock_slack_app, "U12345")

        assert result is None

    def test_get_email_exception(self, mock_slack_app: Any) -> None:
        """Test when Slack API raises exception."""
        mock_slack_app.client.users_info.side_effect = Exception("API error")

        result = _get_slack_email_from_user_id(mock_slack_app, "U12345")

        assert result is None


class TestSearchDataHubUsersBySlackId:
    """Test searching DataHub users by Slack ID."""

    def test_search_finds_user(self, mock_graph: Any) -> None:
        """Test finding a user by Slack ID."""
        mock_graph.get_urns_by_filter.return_value = iter(["urn:li:corpuser:john"])

        result = _search_datahub_users_by_slack_id("U12345", mock_graph)

        assert result == ["urn:li:corpuser:john"]
        mock_graph.get_urns_by_filter.assert_called_once()
        call_args = mock_graph.get_urns_by_filter.call_args
        assert call_args.kwargs["extraFilters"][0]["field"] == "slackUserId"
        assert call_args.kwargs["extraFilters"][0]["value"] == "U12345"
        # Must bypass GMS search cache so freshly-bound users are found immediately
        assert call_args.kwargs["skip_cache"] is True

    def test_search_no_user_found(self, mock_graph: Any) -> None:
        """Test when no user is found by Slack ID."""
        mock_graph.get_urns_by_filter.return_value = iter([])

        result = _search_datahub_users_by_slack_id("U12345", mock_graph)

        assert result == []

    def test_search_multiple_users(self, mock_graph: Any) -> None:
        """Test when multiple users found (edge case)."""
        mock_graph.get_urns_by_filter.return_value = iter(
            ["urn:li:corpuser:john", "urn:li:corpuser:jane"]
        )

        result = _search_datahub_users_by_slack_id("U12345", mock_graph)

        assert len(result) == 2
        assert "urn:li:corpuser:john" in result
        assert "urn:li:corpuser:jane" in result


class TestSearchDataHubUsersByEmail:
    """Test searching DataHub users by email."""

    def test_search_finds_user(self, mock_graph: Any) -> None:
        """Test finding a user by email."""
        mock_graph.get_urns_by_filter.return_value = iter(["urn:li:corpuser:john"])

        result = _search_datahub_users_by_email("john@example.com", mock_graph)

        assert result == ["urn:li:corpuser:john"]
        mock_graph.get_urns_by_filter.assert_called_once()
        call_args = mock_graph.get_urns_by_filter.call_args
        assert call_args.kwargs["extraFilters"][0]["field"] == "email"
        assert call_args.kwargs["extraFilters"][0]["value"] == "john@example.com"

    def test_search_no_user_found(self, mock_graph: Any) -> None:
        """Test when no user is found by email."""
        mock_graph.get_urns_by_filter.return_value = iter([])

        result = _search_datahub_users_by_email("john@example.com", mock_graph)

        assert result == []


class TestGetDataHubUserBySlackId:
    """Test the public API for getting a user by Slack ID."""

    def test_get_user_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test successfully finding a user by Slack ID."""
        mock_search = Mock(return_value=["urn:li:corpuser:john"])
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._search_datahub_users_by_slack_id",
            mock_search,
        )

        result = get_datahub_user_by_slack_id("U12345")

        assert result == "urn:li:corpuser:john"
        mock_search.assert_called_once_with("U12345")

    def test_get_user_not_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test when no user is found."""
        mock_search = Mock(return_value=[])
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._search_datahub_users_by_slack_id",
            mock_search,
        )

        result = get_datahub_user_by_slack_id("U12345")

        assert result is None

    def test_get_user_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test when search raises an exception."""
        mock_search = Mock(side_effect=Exception("Search error"))
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._search_datahub_users_by_slack_id",
            mock_search,
        )

        result = get_datahub_user_by_slack_id("U12345")

        assert result is None


class TestGetUserInformation:
    """Test the main user information retrieval function."""

    def test_oauth_binding_found(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test when user is found via OAuth binding."""
        # Mock OAuth lookup succeeds
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value="urn:li:corpuser:john"),
        )
        # Mock email fetch
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._get_slack_email_from_user_id",
            Mock(return_value="john@example.com"),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app, "U12345", require_oauth_binding=False
        )

        assert email == "john@example.com"
        assert user_urn == "urn:li:corpuser:john"
        assert all_urns == ["urn:li:corpuser:john"]

    def test_oauth_binding_found_no_email(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test when OAuth binding found but email fetch fails."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value="urn:li:corpuser:john"),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._get_slack_email_from_user_id",
            Mock(return_value=None),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app, "U12345", require_oauth_binding=False
        )

        assert email == ""
        assert user_urn == "urn:li:corpuser:john"
        assert all_urns == ["urn:li:corpuser:john"]

    def test_oauth_required_not_found(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test OAuth-only mode when user not found."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value=None),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app,
            "U12345",
            require_oauth_binding=True,  # OAuth required
        )

        assert email == ""
        assert user_urn is None
        assert all_urns == []

    def test_migration_mode_email_fallback_success(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test migration mode falls back to email lookup."""
        # OAuth lookup fails
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value=None),
        )
        # Email lookup succeeds
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._get_slack_email_from_user_id",
            Mock(return_value="john@example.com"),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._search_datahub_users_by_email",
            Mock(return_value=["urn:li:corpuser:john"]),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app,
            "U12345",
            require_oauth_binding=False,  # Migration mode
        )

        assert email == "john@example.com"
        assert user_urn == "urn:li:corpuser:john"
        assert all_urns == ["urn:li:corpuser:john"]

    def test_migration_mode_email_fallback_no_email(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test migration mode when email cannot be fetched."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value=None),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._get_slack_email_from_user_id",
            Mock(return_value=None),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app, "U12345", require_oauth_binding=False
        )

        assert email == ""
        assert user_urn is None
        assert all_urns == []

    def test_migration_mode_email_fallback_no_user(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test migration mode when email found but no matching user."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value=None),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._get_slack_email_from_user_id",
            Mock(return_value="john@example.com"),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user._search_datahub_users_by_email",
            Mock(return_value=[]),
        )

        email, user_urn, all_urns = get_user_information(
            mock_slack_app, "U12345", require_oauth_binding=False
        )

        assert email == "john@example.com"
        assert user_urn is None
        assert all_urns == []


class TestGetDataHubUser:
    """Test the simplified user URN getter."""

    def test_get_user_success(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test successfully getting user URN."""
        mock_get_info = Mock(
            return_value=(
                "john@example.com",
                "urn:li:corpuser:john",
                ["urn:li:corpuser:john"],
            )
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_user_information",
            mock_get_info,
        )

        result = get_datahub_user(mock_slack_app, "U12345", require_oauth_binding=False)

        assert result == "urn:li:corpuser:john"
        mock_get_info.assert_called_once_with(mock_slack_app, "U12345", False)

    def test_get_user_not_found(
        self, mock_slack_app: slack_bolt.App, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test when user not found."""
        mock_get_info = Mock(return_value=("", None, []))
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_user_information",
            mock_get_info,
        )

        result = get_datahub_user(mock_slack_app, "U12345", require_oauth_binding=True)

        assert result is None
        mock_get_info.assert_called_once_with(mock_slack_app, "U12345", True)


class TestGraphAsUser:
    """Test user impersonation graph creation."""

    def test_creates_impersonation_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that impersonation header is set correctly."""
        # Mock the graph.config to avoid actual DataHubGraph instantiation issues
        mock_base_graph = MagicMock(spec=DataHubGraph)
        mock_config = MagicMock()
        mock_copied_config = MagicMock()
        mock_config.model_copy.return_value = mock_copied_config
        mock_base_graph.config = mock_config

        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.graph", mock_base_graph
        )

        # Mock DataHubGraph constructor to avoid instantiation issues
        mock_graph_class = MagicMock()
        mock_graph_instance = MagicMock(spec=DataHubGraph)
        mock_graph_class.return_value = mock_graph_instance

        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.DataHubGraph",
            mock_graph_class,
        )

        result = graph_as_user("urn:li:corpuser:john")

        # Verify model_copy was called with impersonation header
        mock_config.model_copy.assert_called_once()
        call_args = mock_config.model_copy.call_args
        assert "update" in call_args.kwargs
        assert "extra_headers" in call_args.kwargs["update"]
        assert (
            call_args.kwargs["update"]["extra_headers"]["X-DataHub-Impersonated-Urn"]
            == "urn:li:corpuser:john"
        )
        assert call_args.kwargs["deep"] is True

        # Verify DataHubGraph was instantiated with the copied config
        mock_graph_class.assert_called_once_with(mock_copied_config)
        assert result == mock_graph_instance


class TestUserResolutionResult:
    """Tests for UserResolutionResult class."""

    def test_is_resolved_true_when_user_urn_present(self) -> None:
        """is_resolved should be True when user_urn is set."""
        result = UserResolutionResult(user_urn="urn:li:corpuser:testuser")
        assert result.is_resolved is True

    def test_is_resolved_false_when_user_urn_none(self) -> None:
        """is_resolved should be False when user_urn is None."""
        result = UserResolutionResult(user_urn=None)
        assert result.is_resolved is False

    def test_should_prompt_connection_true_when_requires_and_no_user(self) -> None:
        """should_prompt_connection should be True when requires_connection and no user."""
        result = UserResolutionResult(user_urn=None, requires_connection=True)
        assert result.should_prompt_connection is True

    def test_should_prompt_connection_false_when_user_resolved(self) -> None:
        """should_prompt_connection should be False when user is resolved."""
        result = UserResolutionResult(
            user_urn="urn:li:corpuser:testuser", requires_connection=True
        )
        assert result.should_prompt_connection is False

    def test_should_prompt_connection_false_when_not_required(self) -> None:
        """should_prompt_connection should be False when OAuth not required."""
        result = UserResolutionResult(user_urn=None, requires_connection=False)
        assert result.should_prompt_connection is False


class TestResolveSlackUserToDatahub:
    """Tests for resolve_slack_user_to_datahub function."""

    def test_oauth_not_required_returns_no_user_no_connection_required(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When OAuth is not required, should return no user and no connection required."""
        mock_get_user = Mock()
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            mock_get_user,
        )

        result = resolve_slack_user_to_datahub(
            slack_user_id="U12345678",
            require_oauth_binding=False,
        )

        assert result.user_urn is None
        assert result.requires_connection is False
        assert result.should_prompt_connection is False
        mock_get_user.assert_not_called()

    def test_oauth_required_user_found_returns_user_urn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When OAuth required and user found, should return user URN."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value="urn:li:corpuser:testuser"),
        )

        result = resolve_slack_user_to_datahub(
            slack_user_id="U12345678",
            require_oauth_binding=True,
        )

        assert result.user_urn == "urn:li:corpuser:testuser"
        assert result.requires_connection is True
        assert result.is_resolved is True
        assert result.should_prompt_connection is False

    def test_oauth_required_user_not_found_returns_prompt_required(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When OAuth required and user not found, should indicate prompt needed."""
        monkeypatch.setattr(
            "datahub_integrations.slack.utils.datahub_user.get_datahub_user_by_slack_id",
            Mock(return_value=None),
        )

        result = resolve_slack_user_to_datahub(
            slack_user_id="U12345678",
            require_oauth_binding=True,
        )

        assert result.user_urn is None
        assert result.requires_connection is True
        assert result.is_resolved is False
        assert result.should_prompt_connection is True


class TestBuildConnectAccountBlocks:
    """Tests for build_connect_account_blocks function."""

    def test_returns_tuple_of_text_and_blocks(self) -> None:
        """Should return a tuple of (text, blocks)."""
        text, blocks = build_connect_account_blocks("U12345678")

        assert isinstance(text, str)
        assert isinstance(blocks, list)

    def test_text_mentions_user(self) -> None:
        """Should mention the Slack user in the message."""
        text, _ = build_connect_account_blocks("U12345678")

        assert "<@U12345678>" in text

    def test_text_uses_action_description(self) -> None:
        """Should use the action description in the message."""
        text, _ = build_connect_account_blocks(
            "U12345678", action_description="manage incidents"
        )

        assert "manage incidents" in text

    def test_text_uses_default_action_description(self) -> None:
        """Should use default action description when not specified."""
        text, _ = build_connect_account_blocks("U12345678")

        assert "use this feature" in text

    def test_blocks_contain_section_with_text(self) -> None:
        """Should have a section block with the message text."""
        _, blocks = build_connect_account_blocks("U12345678")

        section_blocks = [b for b in blocks if b.get("type") == "section"]
        assert len(section_blocks) >= 1
        assert section_blocks[0]["text"]["type"] == "mrkdwn"

    def test_blocks_contain_connect_button(self) -> None:
        """Should have an actions block with a connect button."""
        _, blocks = build_connect_account_blocks("U12345678")

        action_blocks = [b for b in blocks if b.get("type") == "actions"]
        assert len(action_blocks) >= 1

        elements = action_blocks[0]["elements"]
        assert len(elements) >= 1

        button = elements[0]
        assert button["type"] == "button"
        assert "Connect" in button["text"]["text"]

    def test_connect_button_has_correct_url(self) -> None:
        """Should link to personal notifications settings with connect_slack param."""
        _, blocks = build_connect_account_blocks("U12345678")

        action_blocks = [b for b in blocks if b.get("type") == "actions"]
        button = action_blocks[0]["elements"][0]

        assert "settings/personal-notifications" in button["url"]
        assert "connect_slack=true" in button["url"]

    def test_blocks_contain_context_helper_text(self) -> None:
        """Should have a context block with helper text."""
        _, blocks = build_connect_account_blocks("U12345678")

        context_blocks = [b for b in blocks if b.get("type") == "context"]
        assert len(context_blocks) >= 1


class TestBuildAuthorizationErrorBlocks:
    """Tests for build_authorization_error_blocks function."""

    def test_returns_tuple_of_text_and_blocks(self) -> None:
        """Should return a tuple of (text, blocks)."""
        text, blocks = build_authorization_error_blocks("U12345678")

        assert isinstance(text, str)
        assert isinstance(blocks, list)

    def test_text_mentions_user(self) -> None:
        """Should mention the Slack user in the message."""
        text, _ = build_authorization_error_blocks("U12345678")

        assert "<@U12345678>" in text

    def test_text_contains_permission_denied_message(self) -> None:
        """Should indicate the user doesn't have permission."""
        text, _ = build_authorization_error_blocks("U12345678")

        assert "permission" in text.lower()

    def test_text_uses_action_description(self) -> None:
        """Should use the action description in the message."""
        text, _ = build_authorization_error_blocks(
            "U12345678", action_description="resolve this incident"
        )

        assert "resolve this incident" in text

    def test_text_uses_default_action_description(self) -> None:
        """Should use default action description when not specified."""
        text, _ = build_authorization_error_blocks("U12345678")

        assert "perform this action" in text

    def test_blocks_contain_section_with_text(self) -> None:
        """Should have a section block with the message text."""
        _, blocks = build_authorization_error_blocks("U12345678")

        section_blocks = [b for b in blocks if b.get("type") == "section"]
        assert len(section_blocks) >= 1
        assert section_blocks[0]["text"]["type"] == "mrkdwn"

    def test_blocks_contain_no_entry_emoji(self) -> None:
        """Should include a no_entry emoji in the section text."""
        _, blocks = build_authorization_error_blocks("U12345678")

        section_blocks = [b for b in blocks if b.get("type") == "section"]
        section_text = section_blocks[0]["text"]["text"]
        assert ":no_entry:" in section_text

    def test_blocks_contain_context_helper_text(self) -> None:
        """Should have a context block with helper text."""
        _, blocks = build_authorization_error_blocks("U12345678")

        context_blocks = [b for b in blocks if b.get("type") == "context"]
        assert len(context_blocks) >= 1

    def test_context_mentions_privileges(self) -> None:
        """Should mention that privileges are required."""
        _, blocks = build_authorization_error_blocks("U12345678")

        context_blocks = [b for b in blocks if b.get("type") == "context"]
        context_text = context_blocks[0]["elements"][0]["text"]
        assert "privileges" in context_text.lower()
