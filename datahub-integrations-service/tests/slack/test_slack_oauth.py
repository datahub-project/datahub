"""
Unit tests for Slack OAuth endpoints and helper functions.

Tests the OAuth flow for personal notifications binding.
"""

import base64
import json
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.responses import RedirectResponse


def _make_id_token(claims: dict) -> str:
    """Build a fake JWT ``id_token`` with the given payload claims."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).rstrip(
        b"="
    )
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    signature = base64.urlsafe_b64encode(b"fake-signature").rstrip(b"=")
    return f"{header.decode()}.{payload.decode()}.{signature.decode()}"


# Test fixtures
@pytest.fixture
def mock_slack_config():
    """Create a mock Slack configuration."""
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.client_id = "test-client-id"
    mock_config.app_details.client_secret = "test-client-secret"
    mock_config.bot_token = "xoxb-test-token"
    return mock_config


@pytest.fixture
def mock_slack_config_no_app_details():
    """Create a mock Slack configuration with no app details."""
    mock_config = MagicMock()
    mock_config.app_details = None
    return mock_config


class TestStoreUserSlackSettings:
    """Test storing user Slack settings (minimal binding)."""

    def test_store_binding_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test successfully storing Slack -> DataHub user binding."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None
        mock_graph.emit = MagicMock()

        monkeypatch.setattr("datahub_integrations.slack.slack.graph", mock_graph)

        from datahub_integrations.slack.slack import store_user_slack_settings

        result = store_user_slack_settings(
            user_urn="urn:li:corpuser:john",
            slack_user_id="U12345678",
            display_name="John Doe",
        )

        assert result is True
        mock_graph.emit.assert_called_once()

    def test_store_binding_minimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test storing binding with only required fields."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None
        mock_graph.emit = MagicMock()

        monkeypatch.setattr("datahub_integrations.slack.slack.graph", mock_graph)

        from datahub_integrations.slack.slack import store_user_slack_settings

        result = store_user_slack_settings(
            user_urn="urn:li:corpuser:john",
            slack_user_id="U12345678",
        )

        assert result is True
        mock_graph.emit.assert_called_once()

    def test_store_binding_updates_existing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test updating existing user settings."""
        from datahub.metadata.com.linkedin.pegasus2avro.identity import (
            CorpUserAppearanceSettingsClass,
            CorpUserSettingsClass,
        )
        from datahub.metadata.schema_classes import (
            NotificationSettingsClass,
        )

        existing_settings = CorpUserSettingsClass(
            appearance=CorpUserAppearanceSettingsClass(showSimplifiedHomepage=False),
            notificationSettings=NotificationSettingsClass(sinkTypes=[]),
        )

        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = existing_settings
        mock_graph.emit = MagicMock()

        monkeypatch.setattr("datahub_integrations.slack.slack.graph", mock_graph)

        from datahub_integrations.slack.slack import store_user_slack_settings

        result = store_user_slack_settings(
            user_urn="urn:li:corpuser:john",
            slack_user_id="U12345678",
        )

        assert result is True
        mock_graph.emit.assert_called_once()

    def test_store_binding_preserves_existing_slack_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that updating user binding preserves existing channels and userHandle."""
        from datahub.metadata.com.linkedin.pegasus2avro.identity import (
            CorpUserAppearanceSettingsClass,
            CorpUserSettingsClass,
        )
        from datahub.metadata.schema_classes import (
            NotificationSettingsClass,
            SlackNotificationSettingsClass,
        )

        existing_slack = SlackNotificationSettingsClass(
            userHandle="U_LEGACY",
            channels=["#general", "#alerts"],
        )
        existing_settings = CorpUserSettingsClass(
            appearance=CorpUserAppearanceSettingsClass(showSimplifiedHomepage=False),
            notificationSettings=NotificationSettingsClass(
                sinkTypes=[], slackSettings=existing_slack
            ),
        )

        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = existing_settings
        mock_graph.emit = MagicMock()

        monkeypatch.setattr("datahub_integrations.slack.slack.graph", mock_graph)

        from datahub_integrations.slack.slack import store_user_slack_settings

        result = store_user_slack_settings(
            user_urn="urn:li:corpuser:john",
            slack_user_id="U12345678",
        )

        assert result is True
        emitted_mcp = mock_graph.emit.call_args[0][0]
        slack_settings = emitted_mcp.aspect.notificationSettings.slackSettings
        # New user binding is set
        assert slack_settings.user.slackUserId == "U12345678"
        # Existing channels and userHandle are preserved
        assert slack_settings.channels == ["#general", "#alerts"]
        assert slack_settings.userHandle == "U_LEGACY"

    def test_store_binding_handles_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test handling errors during binding storage."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.side_effect = Exception("Storage error")

        monkeypatch.setattr("datahub_integrations.slack.slack.graph", mock_graph)

        from datahub_integrations.slack.slack import store_user_slack_settings

        result = store_user_slack_settings(
            user_urn="urn:li:corpuser:john",
            slack_user_id="U12345678",
        )

        assert result is False


class TestHandlePersonalNotificationsOAuth:
    """Test the main OAuth handling function."""

    def test_missing_user_urn_with_redirect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling missing user_urn with redirect URL."""
        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="test-code",
            user_urn=None,
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302
        assert "slack_oauth=error" in str(result.headers.get("location", ""))

    def test_missing_user_urn_without_redirect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling missing user_urn without redirect URL."""
        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        with pytest.raises(HTTPException) as exc_info:
            handle_personal_notifications_oauth(
                code="test-code",
                user_urn=None,
                redirect_url=None,
            )

        assert exc_info.value.status_code == 400

    def test_missing_app_credentials(
        self, mock_slack_config_no_app_details, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling missing app credentials."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config_no_app_details),
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="test-code",
            user_urn="urn:li:corpuser:john",
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302

    def test_successful_oauth_flow(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test successful OIDC OAuth flow end-to-end."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        token = _make_id_token(
            {
                "sub": "U12345678",
                "https://slack.com/user_id": "U12345678",
                "name": "John Doe",
            }
        )
        mock_oidc_response = MagicMock()
        mock_oidc_response.get.side_effect = lambda key, default=None: {
            "id_token": token,
        }.get(key, default)
        mock_oidc_response.validate.return_value = mock_oidc_response

        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.return_value = mock_oidc_response

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        mock_store = MagicMock(return_value=True)
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.store_user_slack_settings",
            mock_store,
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="test-code",
            user_urn="urn:li:corpuser:john",
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=success" in location
        assert "U12345678" in location

        mock_web_client.openid_connect_token.assert_called_once()
        mock_store.assert_called_once()
        call_args = mock_store.call_args
        assert call_args[0][0] == "urn:li:corpuser:john"
        assert call_args[1]["slack_user_id"] == "U12345678"
        assert call_args[1]["display_name"] == "John Doe"


class TestExchangeSlackOidcToken:
    """Test the _exchange_slack_oidc_token helper."""

    def test_successful_exchange(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Successful OIDC token exchange returns validated response."""
        mock_response = MagicMock()
        mock_response.validate.return_value = mock_response

        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.return_value = mock_response

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        from datahub_integrations.slack.slack import _exchange_slack_oidc_token

        result = _exchange_slack_oidc_token(
            mock_slack_config, "test-code", "https://example.com/callback"
        )
        assert result is mock_response
        mock_web_client.openid_connect_token.assert_called_once_with(
            client_id="test-client-id",
            client_secret="test-client-secret",
            code="test-code",
            redirect_uri="https://example.com/callback",
        )

    def test_slack_api_error_propagates(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SlackApiError during exchange is raised to caller."""
        import slack_sdk.errors

        error_response = MagicMock()
        error_response.get.return_value = "invalid_code"

        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.side_effect = (
            slack_sdk.errors.SlackApiError("OIDC failed", response=error_response)
        )

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        from datahub_integrations.slack.slack import _exchange_slack_oidc_token

        with pytest.raises(slack_sdk.errors.SlackApiError):
            _exchange_slack_oidc_token(
                mock_slack_config, "bad-code", "https://example.com/callback"
            )


class TestExtractSlackUserFromOidc:
    """Test the _extract_slack_user_from_oidc helper."""

    def test_extracts_user_id_and_name_from_jwt(self) -> None:
        """Extracts user ID and name from id_token JWT claims."""
        token = _make_id_token(
            {
                "sub": "U12345678",
                "https://slack.com/user_id": "U12345678",
                "name": "Alice",
            }
        )
        mock_response = MagicMock()
        mock_response.get.side_effect = lambda key, default=None: {
            "id_token": token,
        }.get(key, default)

        from datahub_integrations.slack.slack import _extract_slack_user_from_oidc

        user_id, display_name = _extract_slack_user_from_oidc(mock_response)
        assert user_id == "U12345678"
        assert display_name == "Alice"

    def test_falls_back_to_sub_claim(self) -> None:
        """Uses ``sub`` when ``https://slack.com/user_id`` is absent."""
        token = _make_id_token({"sub": "U99999999"})
        mock_response = MagicMock()
        mock_response.get.side_effect = lambda key, default=None: {
            "id_token": token,
        }.get(key, default)

        from datahub_integrations.slack.slack import _extract_slack_user_from_oidc

        user_id, display_name = _extract_slack_user_from_oidc(mock_response)
        assert user_id == "U99999999"
        assert display_name is None

    def test_given_name_fallback(self) -> None:
        """Falls back to given_name when name is absent."""
        token = _make_id_token(
            {
                "sub": "U12345678",
                "given_name": "Bob",
            }
        )
        mock_response = MagicMock()
        mock_response.get.side_effect = lambda key, default=None: {
            "id_token": token,
        }.get(key, default)

        from datahub_integrations.slack.slack import _extract_slack_user_from_oidc

        user_id, display_name = _extract_slack_user_from_oidc(mock_response)
        assert user_id == "U12345678"
        assert display_name == "Bob"

    def test_no_id_token(self) -> None:
        """Returns (None, None) when id_token is missing."""
        mock_response = MagicMock()
        mock_response.get.side_effect = lambda key, default=None: {
            "id_token": None,
        }.get(key, default)

        from datahub_integrations.slack.slack import _extract_slack_user_from_oidc

        user_id, display_name = _extract_slack_user_from_oidc(mock_response)
        assert user_id is None
        assert display_name is None

    def test_malformed_jwt_returns_none(self) -> None:
        """Returns (None, None) when id_token is not a valid JWT."""
        mock_response = MagicMock()
        mock_response.get.side_effect = lambda key, default=None: {
            "id_token": "not-a-jwt",
        }.get(key, default)

        from datahub_integrations.slack.slack import _extract_slack_user_from_oidc

        user_id, display_name = _extract_slack_user_from_oidc(mock_response)
        assert user_id is None
        assert display_name is None


class TestBuildOauthErrorRedirect:
    """Test the _build_oauth_error_redirect helper."""

    def test_url_encodes_message(self) -> None:
        """Error messages with special characters are URL-encoded."""
        from datahub_integrations.slack.slack import _build_oauth_error_redirect

        result = _build_oauth_error_redirect("/settings", "bad&stuff=here")
        location = str(result.headers.get("location", ""))
        assert "bad%26stuff%3Dhere" in location
        assert "&stuff=here" not in location  # must not be raw

    def test_basic_redirect(self) -> None:
        """Produces a 302 redirect with slack_oauth=error."""
        from datahub_integrations.slack.slack import _build_oauth_error_redirect

        result = _build_oauth_error_redirect("/foo", "something broke")
        assert result.status_code == 302
        location = str(result.headers.get("location", ""))
        assert location.startswith("/foo?slack_oauth=error&message=")


class TestHandlePersonalNotificationsOAuthErrors:
    """Test error paths in handle_personal_notifications_oauth."""

    def test_oidc_exchange_failure_with_redirect(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SlackApiError during OIDC exchange produces error redirect."""
        import slack_sdk.errors

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        error_response = MagicMock()
        error_response.get.return_value = "invalid_code"
        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.side_effect = (
            slack_sdk.errors.SlackApiError("OIDC failed", response=error_response)
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="bad-code",
            user_urn="urn:li:corpuser:john",
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=error" in location
        assert "OIDC" in location or "invalid_code" in location

    def test_oidc_exchange_failure_without_redirect(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SlackApiError during OIDC exchange raises HTTPException when no redirect."""
        import slack_sdk.errors

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        error_response = MagicMock()
        error_response.get.return_value = "invalid_code"
        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.side_effect = (
            slack_sdk.errors.SlackApiError("OIDC failed", response=error_response)
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        with pytest.raises(HTTPException) as exc_info:
            handle_personal_notifications_oauth(
                code="bad-code",
                user_urn="urn:li:corpuser:john",
                redirect_url=None,
            )
        assert exc_info.value.status_code == 400

    def test_no_user_id_in_oidc_response(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OIDC succeeds but returns no user ID → error redirect."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        mock_oidc_response = MagicMock()
        mock_oidc_response.get.side_effect = lambda key, default=None: {
            "id_token": None,
        }.get(key, default)
        mock_oidc_response.validate.return_value = mock_oidc_response

        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.return_value = mock_oidc_response
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="test-code",
            user_urn="urn:li:corpuser:john",
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=error" in location

    def test_store_failure_produces_error_redirect(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """store_user_slack_settings returning False → error redirect."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        token = _make_id_token(
            {
                "sub": "U12345678",
                "https://slack.com/user_id": "U12345678",
                "name": "John",
            }
        )
        mock_oidc_response = MagicMock()
        mock_oidc_response.get.side_effect = lambda key, default=None: {
            "id_token": token,
        }.get(key, default)
        mock_oidc_response.validate.return_value = mock_oidc_response

        mock_web_client = MagicMock()
        mock_web_client.openid_connect_token.return_value = mock_oidc_response
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_sdk.web.WebClient",
            MagicMock(return_value=mock_web_client),
        )

        monkeypatch.setattr(
            "datahub_integrations.slack.slack.store_user_slack_settings",
            MagicMock(return_value=False),
        )

        from datahub_integrations.slack.slack import handle_personal_notifications_oauth

        result = handle_personal_notifications_oauth(
            code="test-code",
            user_urn="urn:li:corpuser:john",
            redirect_url="/settings/personal-notifications",
        )

        assert isinstance(result, RedirectResponse)
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=error" in location
        assert "save" in location.lower() or "settings" in location.lower()


class TestHandleUserOAuthFlowPaths:
    """Test error and edge-case paths in _handle_user_oauth_flow."""

    def test_error_param_produces_redirect(self) -> None:
        """OAuth error parameter produces redirect, not an exception."""
        from datahub_integrations.slack.slack import _handle_user_oauth_flow

        result = _handle_user_oauth_flow(
            code="test-code",
            state="some-state",
            state_data={
                "flow": "user",
                "user_urn": "urn:li:corpuser:john",
                "redirect_path": "/settings/personal-notifications",
            },
            error="access_denied",
            error_description="The user denied the request",
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=error" in location

    def test_missing_code_produces_redirect(self) -> None:
        """Missing authorization code produces redirect, not an exception."""
        from datahub_integrations.slack.slack import _handle_user_oauth_flow

        result = _handle_user_oauth_flow(
            code=None,
            state="some-state",
            state_data={
                "flow": "user",
                "user_urn": "urn:li:corpuser:john",
                "redirect_path": "/settings/personal-notifications",
            },
            error=None,
            error_description=None,
        )

        assert isinstance(result, RedirectResponse)
        assert result.status_code == 302
        location = str(result.headers.get("location", ""))
        assert "slack_oauth=error" in location


class TestOAuthCallback:
    """Test the unified OAuth callback endpoint."""

    def test_nonce_state_routes_to_user_flow(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """State stored in the shared OAuth store routes to user flow."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import SLACK_USER_OAUTH_FLOW_ID

        state_store = get_state_store()
        nonce = "test-nonce-abc123"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id=SLACK_USER_OAUTH_FLOW_ID,
            redirect_uri="/settings/personal-notifications",
            code_verifier="",
            created_at=time.time(),
        )

        mock_handler = MagicMock(
            return_value=RedirectResponse(
                url="/settings/personal-notifications?slack_oauth=success",
                status_code=302,
            )
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.handle_personal_notifications_oauth",
            mock_handler,
        )

        from datahub_integrations.slack.slack import oauth_callback

        oauth_callback(
            state=nonce, code="test-code", error=None, error_description=None
        )

        mock_handler.assert_called_once()
        call_args = mock_handler.call_args
        assert call_args.kwargs["user_urn"] == "urn:li:corpuser:john"

    def test_nonce_consumed_on_first_use(self) -> None:
        """Using the same nonce twice should fall through to install flow."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import (
            SLACK_USER_OAUTH_FLOW_ID,
            _parse_oauth_state,
        )

        state_store = get_state_store()
        nonce = "test-nonce-replay"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id=SLACK_USER_OAUTH_FLOW_ID,
            redirect_uri="/settings/personal-notifications",
            code_verifier="",
            created_at=time.time(),
        )

        # First use → user flow
        parsed = _parse_oauth_state(nonce)
        assert parsed["flow"] == "user"
        assert parsed["user_urn"] == "urn:li:corpuser:john"

        # Second use → falls through to install (nonce consumed)
        parsed = _parse_oauth_state(nonce)
        assert parsed["flow"] == "install"

    def test_unknown_state_defaults_to_install_flow(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown state string defaults to install flow."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )

        mock_state_store = MagicMock()
        mock_state_store.consume.return_value = False
        monkeypatch.setattr(
            "datahub_integrations.slack.slack._state_store",
            mock_state_store,
        )

        from datahub_integrations.slack.slack import oauth_callback

        with pytest.raises(HTTPException) as exc_info:
            oauth_callback(
                state="some-random-state",
                code="test-code",
                error=None,
                error_description=None,
            )

        assert exc_info.value.status_code == 400

    def test_unsigned_base64_json_not_accepted_as_user_flow(self) -> None:
        """Unsigned base64 JSON should fall through to install flow."""
        state_data = {
            "flow": "user",
            "user_urn": "urn:li:corpuser:attacker",
        }
        state_encoded = base64.b64encode(json.dumps(state_data).encode()).decode()

        from datahub_integrations.slack.slack import _parse_oauth_state

        parsed = _parse_oauth_state(state_encoded)
        assert parsed["flow"] == "install"


class TestIsSafeRedirectPath:
    """Test the _is_safe_redirect_path helper."""

    def test_valid_relative_paths(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("/settings/personal-notifications") is True
        assert _is_safe_redirect_path("/foo/bar") is True
        assert _is_safe_redirect_path("/") is True

    def test_rejects_protocol_relative(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("//evil.com") is False

    def test_rejects_absolute_url(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("https://evil.com") is False

    def test_rejects_backslash(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("/\\evil.com") is False

    def test_rejects_encoded_slash(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("/%2F/evil.com") is False
        assert _is_safe_redirect_path("/%5Cevil.com") is False

    def test_rejects_traversal(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("/foo/../etc/passwd") is False

    def test_rejects_empty_and_none(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("") is False
        assert _is_safe_redirect_path(None) is False

    def test_rejects_no_leading_slash(self) -> None:
        from datahub_integrations.slack.slack import _is_safe_redirect_path

        assert _is_safe_redirect_path("settings") is False


class TestOpenRedirectProtection:
    """Ensure absolute redirect_path values are rejected."""

    def test_absolute_url_redirect_path_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """redirect_path with :// should be replaced with default."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import SLACK_USER_OAUTH_FLOW_ID

        state_store = get_state_store()
        nonce = "test-nonce-open-redirect"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id=SLACK_USER_OAUTH_FLOW_ID,
            redirect_uri="https://evil.com/steal",
            code_verifier="",
            created_at=time.time(),
        )

        mock_handler = MagicMock(
            return_value=RedirectResponse(url="/default", status_code=302)
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.handle_personal_notifications_oauth",
            mock_handler,
        )

        from datahub_integrations.slack.slack import oauth_callback

        oauth_callback(state=nonce, code="test-code")

        call_args = mock_handler.call_args
        assert call_args.kwargs["redirect_url"] == "/settings/personal-notifications"

    def test_double_slash_redirect_path_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """redirect_path starting with // (protocol-relative) should be rejected."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import SLACK_USER_OAUTH_FLOW_ID

        state_store = get_state_store()
        nonce = "test-nonce-double-slash"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id=SLACK_USER_OAUTH_FLOW_ID,
            redirect_uri="//evil.com/steal",
            code_verifier="",
            created_at=time.time(),
        )

        mock_handler = MagicMock(
            return_value=RedirectResponse(url="/default", status_code=302)
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.handle_personal_notifications_oauth",
            mock_handler,
        )

        from datahub_integrations.slack.slack import oauth_callback

        oauth_callback(state=nonce, code="test-code")

        call_args = mock_handler.call_args
        assert call_args.kwargs["redirect_url"] == "/settings/personal-notifications"


class TestSlackConnect:
    """Test the /public/slack/connect endpoint."""

    @pytest.mark.asyncio
    async def test_returns_authorization_url(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that connect returns a valid authorization URL."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        from datahub_integrations.slack.slack import slack_connect

        # Call the endpoint directly (bypassing Depends for unit test)
        result = await slack_connect(user_urn="urn:li:corpuser:alice")

        assert "slack.com" in result.authorization_url
        assert "openid" in result.authorization_url
        assert "state=" in result.authorization_url

    @pytest.mark.asyncio
    async def test_state_stored_and_consumable(
        self, mock_slack_config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that connect stores state that the callback can consume."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config),
        )
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.DATAHUB_FRONTEND_URL",
            "https://datahub.example.com",
        )

        from datahub_integrations.slack.slack import (
            _parse_oauth_state,
            slack_connect,
        )

        result = await slack_connect(user_urn="urn:li:corpuser:alice")

        # Extract the state nonce from the URL
        from urllib.parse import parse_qs, urlparse

        parsed_url = urlparse(result.authorization_url)
        state_nonce = parse_qs(parsed_url.query)["state"][0]

        # Consume it via _parse_oauth_state
        parsed = _parse_oauth_state(state_nonce)
        assert parsed["flow"] == "user"
        assert parsed["user_urn"] == "urn:li:corpuser:alice"

        # Second use → consumed
        parsed = _parse_oauth_state(state_nonce)
        assert parsed["flow"] == "install"

    @pytest.mark.asyncio
    async def test_missing_app_credentials_raises(
        self, mock_slack_config_no_app_details, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that missing credentials returns 500."""
        monkeypatch.setattr(
            "datahub_integrations.slack.slack.slack_config.get_connection",
            MagicMock(return_value=mock_slack_config_no_app_details),
        )

        from datahub_integrations.slack.slack import slack_connect

        with pytest.raises(HTTPException) as exc_info:
            await slack_connect(user_urn="urn:li:corpuser:alice")
        assert exc_info.value.status_code == 500


class TestParseOAuthState:
    """Test the _parse_oauth_state function directly."""

    def test_nonce_in_store_returns_user_flow(self) -> None:
        """Nonce found in shared store returns user flow with trusted data."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import (
            SLACK_USER_OAUTH_FLOW_ID,
            _parse_oauth_state,
        )

        state_store = get_state_store()
        nonce = "test-parse-nonce"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id=SLACK_USER_OAUTH_FLOW_ID,
            redirect_uri="/settings/personal-notifications",
            code_verifier="",
            created_at=time.time(),
        )

        parsed = _parse_oauth_state(nonce)
        assert parsed["flow"] == "user"
        assert parsed["user_urn"] == "urn:li:corpuser:john"
        assert parsed["redirect_path"] == "/settings/personal-notifications"

    def test_unknown_nonce_returns_install_flow(self) -> None:
        """Unknown nonce defaults to install flow."""
        from datahub_integrations.slack.slack import _parse_oauth_state

        parsed = _parse_oauth_state("nonexistent-nonce")
        assert parsed["flow"] == "install"
        assert parsed["raw_state"] == "nonexistent-nonce"

    def test_opaque_state_defaults_to_install(self) -> None:
        """Opaque (non-nonce) state should default to install flow."""
        from datahub_integrations.slack.slack import _parse_oauth_state

        parsed = _parse_oauth_state("some-opaque-install-token")
        assert parsed["flow"] == "install"
        assert parsed["raw_state"] == "some-opaque-install-token"

    def test_non_slack_nonce_falls_through_to_install(self) -> None:
        """Nonces with a non-Slack plugin_id fall through to install flow."""
        import time

        from datahub_integrations.oauth import OAuthState, get_state_store
        from datahub_integrations.slack.slack import _parse_oauth_state

        state_store = get_state_store()
        nonce = "test-mcp-plugin-nonce"
        state_store._store[nonce] = OAuthState(
            user_urn="urn:li:corpuser:john",
            plugin_id="mcp-plugin-xyz",
            redirect_uri="/callback",
            code_verifier="verifier",
            created_at=time.time(),
        )

        parsed = _parse_oauth_state(nonce)
        assert parsed["flow"] == "install"
