from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRequestClass,
)
from slack_sdk import WebClient
from slack_sdk.web.slack_response import SlackResponse

from datahub_integrations.identity.identity_provider import (
    Group,
    IdentityProvider,
    User,
)
from datahub_integrations.notifications.sinks.slack.template_utils import (
    build_incident_status_change_message,
    build_new_incident_message,
)


# Mocks and Fixtures
@pytest.fixture
def mock_client() -> WebClient:
    client = MagicMock(spec=WebClient)
    return client


@pytest.fixture
def identity_provider() -> IdentityProvider:
    provider = MagicMock(spec=IdentityProvider)
    return provider


@pytest.fixture
def notification_request_all_args() -> NotificationRequestClass:
    # Correct instantiation of NotificationRecipientClass
    recipients = [
        NotificationRecipientClass(
            id="recipient1@example.com", type="SLACK_DM"
        ),  # Assuming 'id' is the correct parameter
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",  # Assuming 'template' is a required field
            parameters={
                "owners": '["owner1", "owner2"]',
                "downstreamOwners": '["downstream1"]',
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "Description of incident",
                "actorUrn": "user:123",
            },
        ),
    )


@pytest.fixture
def notification_request_required_args() -> NotificationRequestClass:
    recipients = [
        NotificationRecipientClass(
            id="recipient1@example.com", type="SLACK_DM"
        ),  # Assuming 'id' is the correct parameter
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",  # Assuming 'template' is a required field
            parameters={
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "Description of incident",
            },
        ),
    )


@pytest.fixture
def notification_request_status_change_all_args() -> NotificationRequestClass:
    recipients = [
        NotificationRecipientClass(id="recipient1@example.com", type="SLACK_DM"),
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",
            parameters={
                "owners": '["owner1", "owner2"]',
                "downstreamOwners": '["downstream1"]',
                "entityPath": "/path/to/incident",
                "entityName": "Important Entity",
                "incidentTitle": "Major Outage",
                "incidentDescription": "A critical failure occurred.",
                "prevStatus": "ACTIVE",
                "newStatus": "RESOLVED",
                "actorUrn": "user:456",
                "message": "Issue has been resolved successfully.",
            },
        ),
    )


@pytest.fixture
def notification_request_status_change_required_args() -> NotificationRequestClass:
    recipients = [
        NotificationRecipientClass(id="recipient1@example.com", type="SLACK_DM"),
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",
            parameters={
                "entityPath": "/path/to/incident",
                "entityName": "Important Entity",
                "incidentTitle": "Major Outage",
                "incidentDescription": "A critical failure occurred.",
                "prevStatus": "ACTIVE",
                "newStatus": "RESOLVED",
            },
        ),
    )


def test_build_new_incident_message_success_user_has_slack_ids(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_all_args: NotificationRequestClass,
) -> None:
    batch_get_actors = cast(Any, identity_provider.batch_get_actors)
    batch_get_actors.return_value = {
        "owner1": User(urn="owner1", email="owner1@example.com", slack="U12345"),
        "owner2": User(urn="owner2", email="owner2@example.com", slack="U12346"),
        "downstream1": Group(
            urn="downstream1", slack="G12345"
        ),  # Assuming 'id' and 'slack' are correct parameters
    }
    get_user = cast(Any, identity_provider.get_user)
    get_user.return_value = User(
        urn="user:123",
        email="actor@example.com",
        displayName="Actor Name",
        slack="U67890",
    )

    # Call the function
    result = build_new_incident_message(
        notification_request_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Incident Raised* \n\n"
        "A new incident has been raised on asset <https://base.url/entity/path|Entity Name> by *Actor Name*.\n\n"
        "*Incident Name*: Incident Alert\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: <@U12345>, <@U12346>\n"
        "*Downstream Asset Owners*: <@G12345>"
    )

    assert expected_string == result


def test_build_new_incident_message_success_user_has_email_lookup(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_all_args: NotificationRequestClass,
) -> None:

    batch_get_actors = cast(Any, identity_provider.batch_get_actors)

    # Assuming user with email has already been looked up and cached
    batch_get_actors.return_value = {
        "owner1": User(urn="owner1", email="owner1@example.com"),
        "downstream1": Group(urn="downstream1", displayName="Test Group"),  # None
    }

    response_data = {
        "ok": True,
        "user": {"id": "U12345", "name": "testuser", "email": "owner1@example.com"},
    }
    # Creating a SlackResponse object
    mock_client.users_lookupByEmail.side_effect = SlackResponse(  # type: ignore
        client=mock_client,
        http_verb="GET",
        api_url="users.lookupByEmail",
        req_args={},
        data=response_data,
        status_code=200,
        headers={},
    )

    get_user = cast(Any, identity_provider.get_user)
    get_user.return_value = User(
        urn="user:123",
        email="actor@example.com",
        displayName="Actor Name",
        slack="U67890",
    )

    # Call the function
    result = build_new_incident_message(
        notification_request_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Incident Raised* \n\n"
        "A new incident has been raised on asset <https://base.url/entity/path|Entity Name> by *Actor Name*.\n\n"
        "*Incident Name*: Incident Alert\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: <@U12345>\n"
        "*Downstream Asset Owners*: Test Group"
    )

    assert expected_string == result


def test_build_new_incident_message_success_required_args_only(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_required_args: NotificationRequestClass,
) -> None:

    # Call the function
    result = build_new_incident_message(
        notification_request_required_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Incident Raised* \n\n"
        "A new incident has been raised on asset <https://base.url/entity/path|Entity Name>.\n\n"
        "*Incident Name*: Incident Alert\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: None\n"
        "*Downstream Asset Owners*: None"
    )

    assert expected_string == result


def test_incident_status_change_success(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_status_change_all_args: NotificationRequestClass,
) -> None:

    identity_provider.batch_get_actors.return_value = {  # type: ignore
        "owner1": User(urn="owner1", email="owner1@example.com", slack="U12345"),
        "owner2": User(urn="owner2", email="owner2@example.com", slack="U12346"),
        "downstream1": Group(urn="downstream1", slack="G12347"),
    }

    get_user = cast(Any, identity_provider.get_user)
    get_user.return_value = User(
        urn="user:456",
        email="actor@example.com",
        displayName="Actor Name",
        slack="U67890",
    )

    result = build_incident_status_change_message(
        notification_request_status_change_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":white_check_mark: *Incident Status Changed*\n\n"
        "The status of incident *Major Outage* on asset <https://base.url/path/to/incident|Important Entity> "
        "has changed from *ACTIVE* to *RESOLVED* by *Actor Name*.\n\n"
        "*Message*: Issue has been resolved successfully.\n\n"
        "*Incident Name*: Major Outage\n"
        "*Incident Description*: A critical failure occurred.\n\n"
        "*Asset Owners*: <@U12345>, <@U12346>\n"
        "*Downstream Asset Owners*: <@G12347>"
    )

    assert result == expected_string


def test_incident_status_change_success_required_args_only(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_status_change_required_args: NotificationRequestClass,
) -> None:
    # Setup mocked identity provider response
    identity_provider.batch_get_actors.return_value = {  # type: ignore
        "owner1": User(urn="owner1", email="owner1@example.com", slack="U12345"),
        "owner2": User(urn="owner2", email="owner2@example.com", slack="U12346"),
        "downstream1": Group(urn="downstream1", slack="G12347"),
    }

    # Call the function
    result = build_incident_status_change_message(
        notification_request_status_change_required_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":white_check_mark: *Incident Status Changed*\n\n"
        "The status of incident *Major Outage* on asset <https://base.url/path/to/incident|Important Entity> "
        "has changed from *ACTIVE* to *RESOLVED*.\n\n"
        "*Message*: None\n\n"
        "*Incident Name*: Major Outage\n"
        "*Incident Description*: A critical failure occurred.\n\n"
        "*Asset Owners*: None\n"
        "*Downstream Asset Owners*: None"
    )

    assert result == expected_string


def test_incident_status_change_failure_identity_provider(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_status_change_all_args: NotificationRequestClass,
) -> None:
    # Setup identity provider to raise an exception
    identity_provider.batch_get_actors.side_effect = Exception("Failed to fetch actors")  # type: ignore

    # Call the function with an expectation of failure
    with patch(
        "datahub_integrations.notifications.sinks.slack.template_utils.logger"
    ) as mock_logger:
        result = build_incident_status_change_message(
            notification_request_status_change_all_args,
            identity_provider,
            mock_client,
            "https://base.url",
        )

    # Check logs and response
    mock_logger.exception.assert_called_once_with(
        "Failed to resolve actors from identity provider. Skipping adding them to notification."
    )
    assert "has changed from *ACTIVE* to *RESOLVED*" in result
    assert "*Asset Owners*: None" in result
    assert "*Downstream Asset Owners*: None" in result


# type: ignore[attr]
