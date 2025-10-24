"""
Teams Notification Settings Smoke Test

Tests the Teams notification GraphQL mutations and queries to verify the Teams integration backend functionality.
"""

from typing import Any

from tests.utils import execute_graphql


def test_teams_notification_settings_crud(auth_session):
    """
    Test complete CRUD operations for Teams notification settings.

    1. Get initial notification settings (should be empty)
    2. Update notification settings with Teams configuration
    3. Verify the settings were saved correctly
    4. Update only Teams settings without affecting other settings
    5. Reset settings to default
    """

    # Step 1: Get initial notification settings
    get_query = """
    query GetUserNotificationSettings {
        getUserNotificationSettings {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            teamsSettings { user { teamsUserId, azureUserId, email, displayName, lastUpdated }, channels { id, name } }
            settings { type, value, params { key, value } }
        }
    }
    """

    response = execute_graphql(auth_session, get_query)
    # initial_settings = response["data"]["getUserNotificationSettings"]

    # Step 2: Update notification settings with Teams configuration
    update_mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            teamsSettings { user { teamsUserId, azureUserId, email, displayName, lastUpdated }, channels { id, name } }
            settings { type, value, params { key, value } }
        }
    }
    """

    teams_settings = {
        "sinkTypes": ["TEAMS"],
        "teamsSettings": {"user": {"email": "shirshanka@acryldata.io"}},
        "settings": [
            {"type": "ENTITY_TAG_CHANGE", "value": "ENABLED", "params": []},
            {"type": "ENTITY_OWNER_CHANGE", "value": "ENABLED", "params": []},
            {"type": "DATASET_SCHEMA_CHANGE", "value": "ENABLED", "params": []},
        ],
    }

    variables: dict = {"input": {"notificationSettings": teams_settings}}
    response = execute_graphql(auth_session, update_mutation, variables)

    updated_settings = response["data"]["updateUserNotificationSettings"]

    # Verify Teams settings were saved correctly
    assert updated_settings["sinkTypes"] == ["TEAMS"]
    assert (
        updated_settings["teamsSettings"]["user"]["email"] == "shirshanka@acryldata.io"
    )

    # Verify notification scenarios were saved
    settings_by_type = {s["type"]: s for s in updated_settings["settings"]}
    assert settings_by_type["ENTITY_TAG_CHANGE"]["value"] == "ENABLED"
    assert settings_by_type["ENTITY_OWNER_CHANGE"]["value"] == "ENABLED"
    assert settings_by_type["DATASET_SCHEMA_CHANGE"]["value"] == "ENABLED"

    # Step 3: Verify settings persist by querying again
    response = execute_graphql(auth_session, get_query)
    persisted_settings = response["data"]["getUserNotificationSettings"]

    assert persisted_settings is not None
    assert persisted_settings["sinkTypes"] == ["TEAMS"]
    assert (
        persisted_settings["teamsSettings"]["user"]["email"]
        == "shirshanka@acryldata.io"
    )

    # Step 4: Update with multiple sink types including Teams
    multi_sink_settings = {
        "sinkTypes": ["EMAIL", "TEAMS"],
        "emailSettings": {"email": "test@company.com"},
        "teamsSettings": {
            "user": {"email": "test@company.com"},
            "channels": [{"id": "General"}],
        },
    }

    variables = {"input": {"notificationSettings": multi_sink_settings}}
    response = execute_graphql(auth_session, update_mutation, variables)

    multi_settings = response["data"]["updateUserNotificationSettings"]
    assert set(multi_settings["sinkTypes"]) == {"EMAIL", "TEAMS"}
    assert multi_settings["emailSettings"]["email"] == "test@company.com"
    assert multi_settings["teamsSettings"]["user"]["email"] == "test@company.com"
    assert any(
        channel["id"] == "General"
        for channel in multi_settings["teamsSettings"]["channels"]
    )

    # Step 5: Reset to default (cleanup)
    reset_settings = {
        "sinkTypes": [],
        "emailSettings": {"email": ""},
        "slackSettings": {},
        "teamsSettings": {},
        "settings": [],
    }

    variables = {"input": {"notificationSettings": reset_settings}}
    response = execute_graphql(auth_session, update_mutation, variables)

    # Verify reset worked
    response = execute_graphql(auth_session, get_query)
    final_settings = response["data"]["getUserNotificationSettings"]

    # Settings should be reset or None
    assert final_settings is None or final_settings["sinkTypes"] == []


def test_teams_notification_settings_validation(auth_session):
    """
    Test validation of Teams notification settings input.
    """

    update_mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
            sinkTypes
            teamsSettings { user { teamsUserId, azureUserId, email, displayName, lastUpdated }, channels { id, name } }
        }
    }
    """

    # Test with valid Teams sink type but no Teams settings
    teams_settings: dict[str, Any] = {
        "sinkTypes": ["TEAMS"]
        # No teamsSettings provided
    }

    variables: dict = {"input": {"notificationSettings": teams_settings}}
    execute_graphql(auth_session, update_mutation, variables)

    # Test with Teams settings but no sink type
    teams_settings = {
        "teamsSettings": {"user": {"email": "test@company.com"}},
        "sinkTypes": [],
        # No sinkTypes including TEAMS
    }

    variables = {"input": {"notificationSettings": teams_settings}}
    execute_graphql(auth_session, update_mutation, variables)


def test_teams_notification_scenario_types(auth_session):
    """
    Test Teams notifications with various valid scenario types.
    """

    update_mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
            sinkTypes
            teamsSettings { user { teamsUserId, azureUserId, email, displayName, lastUpdated } }
            settings { type, value }
        }
    }
    """

    # Test with all valid notification scenario types
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

    teams_settings = {
        "sinkTypes": ["TEAMS"],
        "teamsSettings": {"user": {"email": "test@company.com"}},
        "settings": [
            {"type": scenario_type, "value": "ENABLED", "params": []}
            for scenario_type in valid_scenarios
        ],
    }

    variables: dict = {"input": {"notificationSettings": teams_settings}}
    response = execute_graphql(auth_session, update_mutation, variables)

    updated_settings = response["data"]["updateUserNotificationSettings"]

    # Verify all scenarios were saved
    saved_types = {s["type"] for s in updated_settings["settings"]}
    for scenario_type in valid_scenarios:
        assert scenario_type in saved_types, (
            f"Scenario type {scenario_type} was not saved"
        )

    # Cleanup
    reset_settings: dict = {"sinkTypes": [], "teamsSettings": {}, "settings": []}

    variables = {"input": {"notificationSettings": reset_settings}}
    execute_graphql(auth_session, update_mutation, variables)
