import pytest

from tests.utils import delete_urns_from_file, execute_gql, ingest_file_via_rest

test_group_urn = "urn:li:corpGroup:test-settings"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting group settings test data")
    ingest_file_via_rest(auth_session, "tests/settings/group/group_settings_data.json")
    yield
    print("removing group settings test data")
    delete_urns_from_file(graph_client, "tests/settings/group/group_settings_data.json")


def test_get_group_notification_settings(auth_session, ingest_cleanup_data):
    """
    Verify the getGroupNotificationSettings API returns correct data after updates.
    """
    # Step 1: Get the getGroupNotificationSettings API and ensure null notification settings.
    query = """
    query GetGroupNotificationSettings($input: GetGroupNotificationSettingsInput!) {
        getGroupNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    variables: dict[str, object] = {"input": {"groupUrn": test_group_urn}}
    response = execute_gql(auth_session, query, variables)
    assert "errors" not in response, response.get("errors")

    # Ensure there are no settings to start with.
    assert response["data"]["getGroupNotificationSettings"] is None

    # Step 2: Update all notification settings
    mutation = """
    mutation UpdateGroupNotificationSettings($input: UpdateGroupNotificationSettingsInput!) {
        updateGroupNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    new_settings = {
        "sinkTypes": ["EMAIL", "SLACK"],
        "emailSettings": {"email": "test@example.com"},
        "slackSettings": {"userHandle": "U12345", "channels": ["#general"]},
        "settings": [
            {
                "type": "ENTITY_TAG_CHANGE",
                "value": "ENABLED",
                "params": [{"key": "slack.channel", "value": "#alerts"}],
            },
            {"type": "DATASET_SCHEMA_CHANGE", "value": "ENABLED", "params": []},
        ],
    }
    variables = {
        "input": {"notificationSettings": new_settings, "groupUrn": test_group_urn}
    }
    response = execute_gql(auth_session, mutation, variables)
    assert "errors" not in response, response.get("errors")

    # Step 3: Verify the getGroupNotificationSettings API returns updated values
    query = """
    query GetGroupNotificationSettings($input: GetGroupNotificationSettingsInput!) {
        getGroupNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    variables = {"input": {"groupUrn": test_group_urn}}
    response = execute_gql(auth_session, query, variables)
    assert "errors" not in response, response.get("errors")
    settings = response["data"]["getGroupNotificationSettings"]
    assert settings is not None
    assert settings["sinkTypes"] == ["EMAIL", "SLACK"]
    assert settings["emailSettings"]["email"] == "test@example.com"
    assert settings["slackSettings"]["userHandle"] == "U12345"
    assert "#general" in settings["slackSettings"]["channels"]
    assert any(
        s["type"] == "ENTITY_TAG_CHANGE" and s["value"] == "ENABLED"
        for s in settings["settings"]
    )
    assert any(
        s["type"] == "DATASET_SCHEMA_CHANGE" and s["value"] == "ENABLED"
        for s in settings["settings"]
    )

    # Step 3: Reset notification settings to default - notice that this will NOT overwrite scenario settings.
    reset_settings = {
        "sinkTypes": [],
        "emailSettings": {"email": ""},
        "slackSettings": {},
        "settings": [],
    }
    response = execute_gql(
        auth_session,
        mutation,
        {"input": {"notificationSettings": reset_settings, "groupUrn": test_group_urn}},
    )
    assert "errors" not in response, response.get("errors")

    # Step 4: Verify settings are reset
    response = execute_gql(auth_session, query, variables)
    assert "errors" not in response, response.get("errors")
    settings = response["data"]["getGroupNotificationSettings"]
    assert settings is None or settings["sinkTypes"] == []
    assert settings is None or settings["emailSettings"]["email"] == ""

    # Should not overwrite existing scenario settings.
    assert settings is None or len(settings["settings"]) > 0


def test_get_group_notification_settings_group_does_not_exist(
    auth_session, ingest_cleanup_data
):
    """
    Verify the getGroupNotificationSettings API throws an error when group does not exist. ideally it wouldn't be this one.
    """

    # Step 1: Get the getGroupNotificationSettings API
    query = """
    query GetGroupNotificationSettings($input: GetGroupNotificationSettingsInput!) {
        getGroupNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    variables = {"input": {"groupUrn": "urn:li:corpGroup:group-that-does-not-exist"}}
    response = execute_gql(auth_session, query, variables)
    assert "errors" in response, response.get(
        "errors"
    )  # TODO: Report a much more descriptive error here.


def test_update_group_notification_settings(auth_session, ingest_cleanup_data):
    """
    1) Update group notification settings with new settings - include sink types, email, slack, and scenario settings.
    2) Update only scenario settings. Ensure this does not overwrite other settings.
    3) Update only sink and email settings. Ensure this does not overwrite other settings.
    4) Reset notification setting
    """

    # Step 2: Update all notification settings
    mutation = """
    mutation UpdateGroupNotificationSettings($input: UpdateGroupNotificationSettingsInput!) {
        updateGroupNotificationSettings(input: $input) {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    new_settings = {
        "sinkTypes": ["EMAIL", "SLACK"],
        "emailSettings": {"email": "test@example.com"},
        "slackSettings": {"userHandle": "U12345", "channels": ["#general"]},
        "settings": [
            {
                "type": "ENTITY_TAG_CHANGE",
                "value": "ENABLED",
                "params": [{"key": "slack.channel", "value": "#alerts"}],
            },
            {"type": "DATASET_SCHEMA_CHANGE", "value": "ENABLED", "params": []},
        ],
    }
    variables = {
        "input": {"notificationSettings": new_settings, "groupUrn": test_group_urn}
    }
    response = execute_gql(auth_session, mutation, variables)
    assert "errors" not in response, response.get("errors")
    updated_settings = response["data"]["updateGroupNotificationSettings"]
    assert updated_settings["sinkTypes"] == ["EMAIL", "SLACK"]
    assert updated_settings["emailSettings"]["email"] == "test@example.com"
    assert updated_settings["slackSettings"]["userHandle"] == "U12345"
    assert "#general" in updated_settings["slackSettings"]["channels"]
    assert any(
        s["type"] == "ENTITY_TAG_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )
    assert any(
        s["type"] == "DATASET_SCHEMA_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )

    # Step 3: Update scenario settings only
    scenario_update = {
        "settings": [{"type": "NEW_INCIDENT", "value": "DISABLED", "params": []}]
    }
    response = execute_gql(
        auth_session,
        mutation,
        {
            "input": {
                "notificationSettings": scenario_update,
                "groupUrn": test_group_urn,
            }
        },
    )
    updated_settings = response["data"]["updateGroupNotificationSettings"]
    assert any(
        s["type"] == "NEW_INCIDENT" and s["value"] == "DISABLED"
        for s in updated_settings["settings"]
    )

    # Ensure that this did not touch existing scenario settings:
    assert any(
        s["type"] == "ENTITY_TAG_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )
    assert any(
        s["type"] == "DATASET_SCHEMA_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )

    # Ensure other sink type and email settings remain unchanged
    assert updated_settings["sinkTypes"] == ["EMAIL", "SLACK"]
    assert updated_settings["emailSettings"]["email"] == "test@example.com"
    assert updated_settings["slackSettings"]["userHandle"] == "U12345"

    # Step 4: Update sink and email settings only
    sink_email_update = {
        "sinkTypes": ["EMAIL"],
        "emailSettings": {"email": "new@example.com"},
    }
    response = execute_gql(
        auth_session,
        mutation,
        {
            "input": {
                "notificationSettings": sink_email_update,
                "groupUrn": test_group_urn,
            }
        },
    )
    updated_settings = response["data"]["updateGroupNotificationSettings"]
    # Ensure sink type updated
    assert updated_settings["sinkTypes"] == ["EMAIL"]
    assert updated_settings["emailSettings"]["email"] == "new@example.com"

    # But ensure that slack settings remained untouched
    assert updated_settings["slackSettings"]["userHandle"] == "U12345"

    # ...And ensure that scenario settings remained untouched
    assert any(
        s["type"] == "NEW_INCIDENT" and s["value"] == "DISABLED"
        for s in updated_settings["settings"]
    )
    assert any(
        s["type"] == "ENTITY_TAG_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )
    assert any(
        s["type"] == "DATASET_SCHEMA_CHANGE" and s["value"] == "ENABLED"
        for s in updated_settings["settings"]
    )

    # Step 5: Reset notification settings (cleanup)
    reset_settings = {
        "sinkTypes": [],
        "emailSettings": {"email": ""},
        "slackSettings": {},
        "settings": [],
    }
    response = execute_gql(
        auth_session,
        mutation,
        {"input": {"notificationSettings": reset_settings, "groupUrn": test_group_urn}},
    )
    updated_settings = response["data"]["updateGroupNotificationSettings"]
    assert updated_settings["sinkTypes"] == []
    assert updated_settings["emailSettings"]["email"] == ""

    # Should not overwrite existing scenario settings.
    assert updated_settings is None or len(updated_settings["settings"]) > 0
