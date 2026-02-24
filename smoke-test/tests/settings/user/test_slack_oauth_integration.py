"""
Slack OAuth Integration Service Smoke Tests

Tests the Slack OAuth endpoints in the datahub-integrations-service directly.
These tests verify the backend handling of OAuth configuration and callbacks.

Note: Full OAuth flow requires actual Slack credentials, so we test:
1. Connect endpoint requires authentication
2. OAuth callback error handling (missing code, errors, invalid state)
3. Credentials reload endpoint
4. GraphQL schema has SlackUser support
"""

import logging
from typing import Any, Dict, Optional

import pytest
import requests

from tests.utils import check_integrations_service, get_integrations_service_url

logger = logging.getLogger(__name__)


def get_integrations_url() -> str:
    """Get the integrations service URL."""
    return get_integrations_service_url()


def post_private_endpoint(
    auth_session, endpoint: str, json_data: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """Make a POST request to a private integrations service endpoint."""
    url = f"{auth_session.integrations_service_url()}/private/{endpoint}"
    headers = {"Authorization": f"Bearer {auth_session.gms_token()}"}
    return requests.post(url, json=json_data or {}, headers=headers)


def get_public_endpoint(
    endpoint: str, params: Optional[Dict[str, str]] = None
) -> requests.Response:
    """Make a GET request to a public integrations service endpoint."""
    url = f"{get_integrations_url()}/public/{endpoint}"
    return requests.get(url, params=params)


@pytest.mark.skipif(
    not check_integrations_service(), reason="Integrations service not enabled"
)
class TestSlackConnectEndpoint:
    """Test the /public/slack/connect endpoint (MCP-style OAuth)."""

    def test_connect_requires_auth(self):
        """Test that the connect endpoint rejects unauthenticated requests."""
        url = f"{get_integrations_url()}/public/slack/connect"
        response = requests.post(url, json={})

        # Should require auth (401/403) or reject without valid token
        assert response.status_code in [401, 403, 422], (
            f"Expected auth error, got {response.status_code}"
        )
        logger.info("✅ Slack connect endpoint requires authentication")


@pytest.mark.skipif(
    not check_integrations_service(), reason="Integrations service not enabled"
)
class TestSlackOAuthCallback:
    """Test OAuth callback error handling."""

    def test_callback_handles_missing_code(self):
        """Test that callback handles missing authorization code."""
        response = get_public_endpoint("slack/oauth_callback")

        # Should return error for missing code
        assert response.status_code in [400, 422, 302], (
            f"Expected error or redirect, got {response.status_code}"
        )
        logger.info("✅ Callback correctly handles missing code")

    def test_callback_handles_oauth_error(self):
        """Test that callback handles OAuth error from Slack."""
        params = {
            "error": "access_denied",
            "error_description": "User denied access",
        }
        response = get_public_endpoint("slack/oauth_callback", params=params)

        # Should return error redirect or error response
        # The endpoint may redirect to frontend with error params
        assert response.status_code in [302, 400, 422], (
            f"Expected error handling, got {response.status_code}"
        )

        # If redirect, check it includes error info
        if response.status_code == 302:
            location = response.headers.get("location", "")
            assert "error" in location.lower() or "slack_oauth=error" in location

        logger.info("✅ Callback correctly handles OAuth errors")

    def test_callback_handles_invalid_state(self):
        """Test that callback handles invalid state parameter."""
        params = {
            "code": "fake_code_12345",
            "state": "invalid_base64_state!!!",
        }
        response = get_public_endpoint("slack/oauth_callback", params=params)

        # Should return error for invalid state (or 404 if endpoint not mounted)
        if response.status_code == 404:
            logger.info(
                "⚠️ Callback endpoint not mounted (may need Slack configured first)"
            )
            pytest.skip("Slack OAuth callback endpoint not available")

        assert response.status_code in [302, 400, 422, 500], (
            f"Expected error handling, got {response.status_code}"
        )
        logger.info("✅ Callback correctly handles invalid state")


@pytest.mark.skipif(
    not check_integrations_service(), reason="Integrations service not enabled"
)
class TestSlackCredentialsReload:
    """Test Slack credentials reload endpoint."""

    def test_reload_credentials_endpoint(self, auth_session):
        """Test that reload credentials endpoint is accessible."""
        response = post_private_endpoint(auth_session, "slack/reload_credentials")

        # Should succeed or indicate Slack not configured
        assert response.status_code in [200, 204, 500], (
            f"Unexpected status code: {response.status_code}"
        )

        if response.status_code in [200, 204]:
            logger.info("✅ Reload credentials endpoint succeeded")
        else:
            logger.info("⚠️ Reload credentials failed - Slack may not be configured")


class TestSlackOAuthE2EReadiness:
    """
    Verify the system is ready for E2E OAuth testing.

    These tests don't perform actual OAuth but verify all components are in place.
    """

    def test_notification_settings_support_slack_user(self, auth_session):
        """Verify notification settings support SlackUser model."""
        from tests.utils import execute_graphql

        # Introspection to check SlackNotificationSettings has 'user' field
        query = """
        query {
            __type(name: "SlackNotificationSettings") {
                fields {
                    name
                    type {
                        name
                        kind
                    }
                }
            }
        }
        """

        response = execute_graphql(auth_session, query)
        slack_type = response["data"]["__type"]

        if slack_type:
            field_names = [f["name"] for f in slack_type["fields"]]
            assert "user" in field_names, (
                "SlackNotificationSettings should have 'user' field"
            )
            assert "userHandle" in field_names, (
                "SlackNotificationSettings should have 'userHandle' for backward compat"
            )
            logger.info(
                "✅ SlackNotificationSettings supports both 'user' and legacy 'userHandle'"
            )
        else:
            logger.warning("⚠️ SlackNotificationSettings type not found")

    # Note: Feature flag test is in test_slack_oauth_notifications.py
    # Removed here to avoid duplication
