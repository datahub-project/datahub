from tests.utils import execute_gql


def test_get_user_notification_settings(auth_session):
    """
    Verify the getUserNotificationSettings API returns correct data after updates.
    """
    # Step 1: Update all notification settings
    mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
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
    variables = {"input": {"notificationSettings": new_settings}}
    response = execute_gql(auth_session, mutation, variables)
    assert "errors" not in response, response.get("errors")

    # Step 2: Verify the getUserNotificationSettings API returns updated values
    query = """
    query GetUserNotificationSettings {
        getUserNotificationSettings {
            sinkTypes
            emailSettings { email }
            slackSettings { userHandle, channels }
            settings { type, value, params { key, value } }
        }
    }
    """
    response = execute_gql(auth_session, query)
    assert "errors" not in response, response.get("errors")
    settings = response["data"]["getUserNotificationSettings"]
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
        auth_session, mutation, {"input": {"notificationSettings": reset_settings}}
    )
    assert "errors" not in response, response.get("errors")

    # Step 4: Verify settings are reset
    response = execute_gql(auth_session, query)
    assert "errors" not in response, response.get("errors")
    settings = response["data"]["getUserNotificationSettings"]
    assert settings is None or settings["sinkTypes"] == []
    assert settings is None or settings["emailSettings"]["email"] == ""

    # Should not overwrite existing scenario settings.
    assert settings is None or len(settings["settings"]) > 0


def test_update_user_notification_settings(auth_session):
    """
    1) Update user notification settings with new settings - include sink types, email, slack, and scenario settings.
    2) Update only scenario settings. Ensure this does not overwrite other settings.
    3) Update only sink and email settings. Ensure this does not overwrite other settings.
    4) Reset notification setting
    """

    # Step 2: Update all notification settings
    mutation = """
    mutation UpdateUserNotificationSettings($input: UpdateUserNotificationSettingsInput!) {
        updateUserNotificationSettings(input: $input) {
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
    variables = {"input": {"notificationSettings": new_settings}}
    response = execute_gql(auth_session, mutation, variables)
    assert "errors" not in response, response.get("errors")
    updated_settings = response["data"]["updateUserNotificationSettings"]
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
        auth_session, mutation, {"input": {"notificationSettings": scenario_update}}
    )
    updated_settings = response["data"]["updateUserNotificationSettings"]
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
        auth_session, mutation, {"input": {"notificationSettings": sink_email_update}}
    )
    updated_settings = response["data"]["updateUserNotificationSettings"]
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
        auth_session, mutation, {"input": {"notificationSettings": reset_settings}}
    )
    updated_settings = response["data"]["updateUserNotificationSettings"]
    assert updated_settings["sinkTypes"] == []
    assert updated_settings["emailSettings"]["email"] == ""

    # Should not overwrite existing scenario settings.
    assert updated_settings is None or len(updated_settings["settings"]) > 0
