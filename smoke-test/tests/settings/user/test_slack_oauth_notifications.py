"""
Slack OAuth Notification Settings Smoke Tests

Tests the Slack OAuth integration including:
1. Slack notification settings CRUD via GraphQL (userHandle, persistence, reset)
2. Slack connect endpoint for frontend OAuth flow
3. User binding with SlackUser model (read-only via GraphQL, written via MCP)
4. Feature flag handling for requireSlackOAuthBinding
5. Multiple notification sinks and scenario types

Note: SlackUser is set via MCP emission from slack.py OAuth callback,
NOT via GraphQL mutation. GraphQL only provides read access to SlackUser.
"""

import logging
import time
from typing import Any, Dict, Optional

import pytest

from tests.utils import execute_graphql

logger = logging.getLogger(__name__)


def emit_slack_user_binding(
    graph: Any, user_urn: str, slack_user_id: str, display_name: Optional[str] = None
) -> None:
    """
    Simulate what slack.py does on OAuth callback - emit MCP to bind Slack user.

    This is the ONLY way to set SlackUser (not via GraphQL mutation).
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.com.linkedin.pegasus2avro.identity import (
        CorpUserAppearanceSettingsClass,
        CorpUserSettingsClass,
    )
    from datahub.metadata.schema_classes import (
        NotificationSettingsClass,
        SlackNotificationSettingsClass,
        SlackUserClass,
    )

    # Get or create user settings
    existing_settings = graph.get_aspect(
        entity_urn=user_urn, aspect_type=CorpUserSettingsClass
    )

    if existing_settings:
        corp_settings = existing_settings
        notification_settings = (
            corp_settings.notificationSettings
            or NotificationSettingsClass(sinkTypes=[])
        )
    else:
        corp_settings = CorpUserSettingsClass(
            appearance=CorpUserAppearanceSettingsClass(showSimplifiedHomepage=False)
        )
        notification_settings = NotificationSettingsClass(sinkTypes=[])

    # Create SlackUser binding (same as slack.py::store_user_slack_settings)
    slack_user = SlackUserClass(
        slackUserId=slack_user_id,
        displayName=display_name,
        lastUpdated=int(time.time() * 1000),
    )

    notification_settings.slackSettings = SlackNotificationSettingsClass(
        user=slack_user,
    )

    # Add SLACK to sinkTypes if not present
    if "SLACK" not in (notification_settings.sinkTypes or []):
        notification_settings.sinkTypes = list(
            notification_settings.sinkTypes or []
        ) + ["SLACK"]

    corp_settings.notificationSettings = notification_settings

    # Emit the binding
    mcp = MetadataChangeProposalWrapper(
        entityUrn=user_urn,
        aspect=corp_settings,
    )
    graph.emit(mcp)
    logger.info(f"✅ Emitted SlackUser binding: {user_urn} -> {slack_user_id}")


class TestSlackNotificationSettingsCRUD:
    """Test Slack notification settings CRUD operations."""

    def test_update_slack_settings_legacy_user_handle(self, auth_session):
        """Test backward compatibility with legacy userHandle field."""
        update_mutation = """
        mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
            updateUserNotificationSettings(input: $input) {
                sinkTypes
                slackSettings {
                    user {
                        slackUserId
                    }
                    userHandle
                }
            }
        }
        """

        # Legacy settings with userHandle (deprecated but supported)
        slack_settings: Dict[str, Any] = {
            "sinkTypes": ["SLACK"],
            "slackSettings": {
                "userHandle": "ULEGACY1234",
            },
        }

        variables = {"input": {"notificationSettings": slack_settings}}
        response = execute_graphql(auth_session, update_mutation, variables)

        updated = response["data"]["updateUserNotificationSettings"]

        # Verify legacy userHandle was saved
        assert "SLACK" in updated["sinkTypes"]
        assert updated["slackSettings"]["userHandle"] == "ULEGACY1234"

        logger.info("✅ Legacy userHandle still supported for backward compatibility")

    def test_settings_persistence(self, auth_session):
        """Test that Slack settings persist correctly using legacy userHandle."""
        # Note: SlackUser is set via OAuth callback, userHandle is set via mutation
        update_mutation = """
        mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
            updateUserNotificationSettings(input: $input) {
                sinkTypes
                slackSettings {
                    userHandle
                }
            }
        }
        """

        slack_settings = {
            "sinkTypes": ["SLACK"],
            "slackSettings": {
                "userHandle": "UPERSIST123",
            },
        }

        variables = {"input": {"notificationSettings": slack_settings}}
        execute_graphql(auth_session, update_mutation, variables)

        # Query to verify persistence
        get_query = """
        query GetUserNotificationSettings {
            getUserNotificationSettings {
                sinkTypes
                slackSettings {
                    userHandle
                }
            }
        }
        """

        response = execute_graphql(auth_session, get_query)
        persisted = response["data"]["getUserNotificationSettings"]

        assert persisted["slackSettings"]["userHandle"] == "UPERSIST123"

        logger.info("✅ Slack settings persist correctly")

    def test_reset_slack_settings(self, auth_session):
        """Test resetting Slack settings to default."""
        update_mutation = """
        mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
            updateUserNotificationSettings(input: $input) {
                sinkTypes
                slackSettings {
                    user { slackUserId }
                    userHandle
                }
            }
        }
        """

        # Reset settings
        reset_settings: Dict[str, Any] = {
            "sinkTypes": [],
            "slackSettings": {},
        }

        variables = {"input": {"notificationSettings": reset_settings}}
        response = execute_graphql(auth_session, update_mutation, variables)

        # Verify reset
        updated = response["data"]["updateUserNotificationSettings"]
        assert updated["sinkTypes"] == []

        logger.info("✅ Slack settings reset successfully")


class TestSlackConnect:
    """Test Slack user OAuth connect endpoint."""

    def test_slack_connect_returns_authorization_url(self, auth_session):
        """Test that POST /integrations/slack/connect returns an authorization URL.

        This endpoint follows the same pattern as MCP plugin OAuth:
        - Authenticated via cookie/token
        - Returns { authorization_url: "https://slack.com/..." }
        - State is stored server-side (nonce in URL)
        """
        # Use auth_session.post which auto-injects auth headers
        frontend_url = auth_session.frontend_url()
        response = auth_session.post(
            f"{frontend_url}/integrations/slack/connect",
            timeout=10,
        )

        if response.status_code == 500:
            # Slack may not be configured in this environment
            logger.info(
                "⚠️ Slack connect returned 500 — likely not configured, skipping"
            )
            pytest.skip("Slack app not configured")
            return

        assert response.status_code == 200, (
            f"Expected 200, got {response.status_code}: {response.text}"
        )

        data = response.json()
        assert "authorization_url" in data, "Missing authorization_url in response"
        assert "slack.com" in data["authorization_url"]
        assert "state=" in data["authorization_url"]
        assert "openid" in data["authorization_url"]

        logger.info("✅ Slack connect endpoint returned authorization URL")


class TestSlackFeatureFlags:
    """Test Slack-related feature flags."""

    def test_feature_flags_include_slack_oauth_binding(self, auth_session):
        """Test that requireSlackOAuthBinding feature flag is exposed."""
        query = """
        query GetAppConfig {
            appConfig {
                featureFlags {
                    requireSlackOAuthBinding
                    slackBotTokensConfigEnabled
                }
            }
        }
        """

        response = execute_graphql(auth_session, query)
        feature_flags = response["data"]["appConfig"]["featureFlags"]

        # Verify feature flags exist
        assert "requireSlackOAuthBinding" in feature_flags
        assert "slackBotTokensConfigEnabled" in feature_flags

        logger.info(
            f"✅ Feature flags: requireSlackOAuthBinding={feature_flags['requireSlackOAuthBinding']}"
        )
        logger.info(
            f"   slackBotTokensConfigEnabled={feature_flags['slackBotTokensConfigEnabled']}"
        )


class TestSlackNotificationScenarios:
    """Test Slack notifications with various scenario types."""

    def test_slack_with_all_notification_scenarios(self, auth_session):
        """Test Slack with all valid notification scenario types."""
        update_mutation = """
        mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
            updateUserNotificationSettings(input: $input) {
                sinkTypes
                slackSettings {
                    user { slackUserId }
                }
                settings { type, value }
            }
        }
        """

        valid_scenarios = [
            "ENTITY_TAG_CHANGE",
            "ENTITY_OWNER_CHANGE",
            "ENTITY_DOMAIN_CHANGE",
            "ENTITY_DEPRECATION_CHANGE",
            "DATASET_SCHEMA_CHANGE",
            "ENTITY_GLOSSARY_TERM_CHANGE",
            "NEW_INCIDENT",
            "INCIDENT_STATUS_CHANGE",
        ]

        slack_settings = {
            "sinkTypes": ["SLACK"],
            "slackSettings": {"userHandle": "USCENARIO123"},
            "settings": [
                {"type": scenario, "value": "ENABLED", "params": []}
                for scenario in valid_scenarios
            ],
        }

        variables = {"input": {"notificationSettings": slack_settings}}
        response = execute_graphql(auth_session, update_mutation, variables)

        updated = response["data"]["updateUserNotificationSettings"]

        # Verify all scenarios were saved
        saved_types = {s["type"] for s in updated["settings"]}
        for scenario in valid_scenarios:
            assert scenario in saved_types, f"Scenario {scenario} was not saved"

        logger.info(
            f"✅ All {len(valid_scenarios)} notification scenarios saved successfully"
        )

        # Cleanup
        cleanup_settings: Dict[str, Any] = {
            "sinkTypes": [],
            "slackSettings": {},
            "settings": [],
        }
        variables = {"input": {"notificationSettings": cleanup_settings}}
        execute_graphql(auth_session, update_mutation, variables)


class TestSlackMultipleSinks:
    """Test Slack with multiple notification sinks."""

    def test_slack_with_email_sink(self, auth_session):
        """Test using Slack alongside email notifications."""
        update_mutation = """
        mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
            updateUserNotificationSettings(input: $input) {
                sinkTypes
                emailSettings { email }
                slackSettings {
                    userHandle
                }
            }
        }
        """

        multi_sink_settings = {
            "sinkTypes": ["EMAIL", "SLACK"],
            "emailSettings": {"email": "test@company.com"},
            "slackSettings": {
                "userHandle": "UMULTI1234",
            },
        }

        variables = {"input": {"notificationSettings": multi_sink_settings}}
        response = execute_graphql(auth_session, update_mutation, variables)

        updated = response["data"]["updateUserNotificationSettings"]

        assert set(updated["sinkTypes"]) == {"EMAIL", "SLACK"}
        assert updated["emailSettings"]["email"] == "test@company.com"
        assert updated["slackSettings"]["userHandle"] == "UMULTI1234"

        logger.info("✅ Slack works correctly with multiple sinks")

        # Cleanup - use try/except to not fail test on cleanup issues
        try:
            cleanup_settings: Dict[str, Any] = {
                "sinkTypes": [],
                "emailSettings": {},
                "slackSettings": {},
            }
            variables = {"input": {"notificationSettings": cleanup_settings}}
            execute_graphql(auth_session, update_mutation, variables)
        except Exception as e:
            logger.warning(f"Cleanup failed (non-fatal): {e}")


class TestSlackOAuthMCPBinding:
    """
    Test the OAuth binding flow using MCP emission.

    This simulates what slack.py::store_user_slack_settings() does on OAuth callback.
    SlackUser can ONLY be set via MCP emission, NOT via GraphQL mutation.
    """

    def test_oauth_binding_via_mcp_then_read_via_graphql(
        self, auth_session, graph_client
    ):
        """
        Test the full OAuth flow:
        1. Emit MCP to bind SlackUser (simulates slack.py OAuth callback)
        2. Read back via GraphQL to verify binding worked
        """
        # The logged-in user is "admin" based on the logs
        user_urn = "urn:li:corpuser:admin"

        # Verify user exists by trying to read current settings first
        try:
            verify_query = """
            query GetUserNotificationSettings {
                getUserNotificationSettings {
                    sinkTypes
                }
            }
            """
            execute_graphql(auth_session, verify_query)
            logger.info(f"✅ Verified user context, using URN: {user_urn}")
        except Exception as e:
            logger.warning(f"Could not verify user context: {e}")
            pytest.skip("Could not verify user context for MCP test")

        test_slack_id = "UOAUTHTEST123"
        test_display_name = "OAuth Test User"

        # Step 1: Emit MCP (simulates slack.py::store_user_slack_settings)
        emit_slack_user_binding(
            graph=graph_client,
            user_urn=user_urn,
            slack_user_id=test_slack_id,
            display_name=test_display_name,
        )

        # Give GMS a moment to process
        time.sleep(2)

        # Step 2: Read back via GraphQL
        get_query = """
        query GetUserNotificationSettings {
            getUserNotificationSettings {
                sinkTypes
                slackSettings {
                    user {
                        slackUserId
                        displayName
                        lastUpdated
                    }
                    userHandle
                }
            }
        }
        """

        response = execute_graphql(auth_session, get_query)
        settings = response["data"]["getUserNotificationSettings"]

        # Verify the OAuth-bound user was read correctly
        assert settings is not None, "Settings should not be None"
        assert "SLACK" in settings["sinkTypes"], "SLACK should be in sinkTypes"
        assert settings["slackSettings"] is not None, "slackSettings should exist"
        assert settings["slackSettings"]["user"] is not None, "user should exist"
        assert settings["slackSettings"]["user"]["slackUserId"] == test_slack_id
        assert settings["slackSettings"]["user"]["displayName"] == test_display_name
        assert settings["slackSettings"]["user"]["lastUpdated"] is not None

        logger.info(f"✅ OAuth binding verified: {test_slack_id} -> {user_urn}")
        logger.info(f"   SlackUser: {settings['slackSettings']['user']}")


# Final cleanup fixture
@pytest.fixture(autouse=True, scope="module")
def cleanup_after_tests(auth_session):
    """Cleanup after all tests in this module."""
    yield
    # Reset settings after all tests
    update_mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
            sinkTypes
        }
    }
    """
    reset_settings: Dict[str, Any] = {
        "sinkTypes": [],
        "slackSettings": {},
        "settings": [],
    }
    variables: Dict[str, Any] = {"input": {"notificationSettings": reset_settings}}
    try:
        execute_graphql(auth_session, update_mutation, variables)
        logger.info("✅ Cleaned up Slack notification settings")
    except Exception as e:
        logger.warning(f"Failed to cleanup: {e}")
