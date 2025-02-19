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
    build_incident_message,
    build_incident_status_change_message,
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
                "owners": '["owner1", "owner2", "owner3", "owner4", "owner5", "owner6", "owner7", "owner8", "owner9", "owner10", "owner11"]',
                "downstreamOwners": '["downstream1"]',
                "downstreamAssetsCount": "20",
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "entityType": "Table",
                "entityPlatform": "Snowflake",
                "incidentUrn": "urn:li:incident:test",
                "incidentType": "FRESHNESS",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "Description of incident",
                "incidentPriority": "1",
                "incidentStage": "WORK_IN_PROGRESS",
                "actorUrn": "user:123",
                "assertionUrn": "urn:li:assertion:test",
                "assertionDescription": "Table was not updated in past 6 hours",
                "assertionType": "FRESHNESS",
                "assertionSourceType": "NATIVE",
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
def notification_request_resolved_all_args() -> NotificationRequestClass:
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
                "downstreamAssetsCount": "20",
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "entityType": "Table",
                "entityPlatform": "Snowflake",
                "incidentUrn": "urn:li:incident:test",
                "incidentType": "FRESHNESS",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "Description of incident",
                "incidentPriority": "1",
                "incidentStage": "WORK_IN_PROGRESS",
                "actorUrn": "user:456",
                "assertionUrn": "urn:li:assertion:test",
                "assertionDescription": "Table was not updated in past 6 hours",
                "assertionType": "FRESHNESS",
                "assertionSourceType": "NATIVE",
                "prevStatus": "ACTIVE",
                "newStatus": "RESOLVED",
                "message": "Issue has been resolved successfully.",
            },
        ),
    )


@pytest.fixture
def notification_request_resolved_required_args() -> NotificationRequestClass:
    recipients = [
        NotificationRecipientClass(id="recipient1@example.com", type="SLACK_DM"),
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",
            parameters={
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "A critical failure occurred.",
                "prevStatus": "ACTIVE",
                "newStatus": "RESOLVED",
            },
        ),
    )


@pytest.fixture
def notification_request_reopened_all_args() -> NotificationRequestClass:
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
                "downstreamAssetsCount": "20",
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "entityType": "Table",
                "entityPlatform": "Snowflake",
                "incidentUrn": "urn:li:incident:test",
                "incidentType": "FRESHNESS",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "Description of incident",
                "incidentPriority": "1",
                "incidentStage": "WORK_IN_PROGRESS",
                "assertionUrn": "urn:li:assertion:test",
                "assertionDescription": "Table was not updated in past 6 hours",
                "assertionType": "FRESHNESS",
                "assertionSourceType": "NATIVE",
                "prevStatus": "RESOLVED",
                "newStatus": "ACTIVE",
                "actorUrn": "user:456",
                "message": "Issue has been resolved successfully.",
            },
        ),
    )


@pytest.fixture
def notification_request_reopened_required_args() -> NotificationRequestClass:
    recipients = [
        NotificationRecipientClass(id="recipient1@example.com", type="SLACK_DM"),
        NotificationRecipientClass(id="recipient2@example.com", type="SLACK_DM"),
    ]
    return NotificationRequestClass(
        recipients=recipients,
        message=NotificationMessageClass(
            template="template_id",
            parameters={
                "entityPath": "/entity/path",
                "entityName": "Entity Name",
                "incidentTitle": "Incident Alert",
                "incidentDescription": "A critical failure occurred.",
                "prevStatus": "RESOLVED",
                "newStatus": "ACTIVE",
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
        "owner3": User(urn="owner3", email="owner3@example.com", slack="U12347"),
        "owner4": User(urn="owner4", email="owner4@example.com", slack="U12348"),
        "owner5": User(urn="owner5", email="owner5@example.com", slack="U12349"),
        "owner6": User(urn="owner6", email="owner6@example.com", slack="U12350"),
        "owner7": User(urn="owner7", email="owner7@example.com", slack="U12351"),
        "owner8": User(urn="owner8", email="owner8@example.com", slack="U12352"),
        "owner9": User(urn="owner9", email="owner9@example.com", slack="U12353"),
        "owner10": User(urn="owner10", email="owner10@example.com", slack="U12354"),
        "owner11": User(urn="owner11", email="owner11@example.com", slack="U12355"),
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
    text, blocks, attachments = build_incident_message(
        notification_request_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Data Incident* \n\n"
        "An incident has been raised on asset <https://base.url/entity/path/Incidents|Entity Name> by *Actor Name*.\n\n"
        "*Incident Name*: Freshness Assertion <https://base.url/entity/path/Validation/Assertions?assertion_urn=urn:li:assertion:test|Table was not updated in past 6 hours> has failed\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: <@U12345>, <@U12346>, <@U12347>, <@U12348>, <@U12349>, <@U12350>, <@U12351>, <@U12352>, <@U12353>, <@U12354>, + 1 more\n"
        "*Impacted Asset Owners*: <@G12345>"
    )

    assert expected_string == text

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 1
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


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
    text, blocks, attachments = build_incident_message(
        notification_request_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Data Incident* \n\n"
        "An incident has been raised on asset <https://base.url/entity/path/Incidents|Entity Name> by *Actor Name*.\n\n"
        "*Incident Name*: Freshness Assertion <https://base.url/entity/path/Validation/Assertions?assertion_urn=urn:li:assertion:test|Table was not updated in past 6 hours> has failed\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: <@U12345>\n"
        "*Impacted Asset Owners*: Test Group"
    )

    assert expected_string == text

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 1
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_build_new_incident_message_success_required_args_only(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_required_args: NotificationRequestClass,
) -> None:
    # Call the function
    text, blocks, attachments = build_incident_message(
        notification_request_required_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *New Data Incident* \n\n"
        "An incident has been raised on asset <https://base.url/entity/path/Incidents|Entity Name>.\n\n"
        "*Incident Name*: Incident Alert\n"
        "*Incident Description*: Description of incident\n\n"
        "*Asset Owners*: None\n"
        "*Impacted Asset Owners*: None"
    )

    assert expected_string == text

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 1
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_incident_resolved_success(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_resolved_all_args: NotificationRequestClass,
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

    text, blocks, attachments = build_incident_status_change_message(
        notification_request_resolved_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":white_check_mark: *Data Incident Resolved*\n\n"
        "Incident *Freshness Assertion <https://base.url/entity/path/Validation/Assertions?assertion_urn=urn:li:assertion:test|Table was not updated in past 6 hours> has failed* on asset <https://base.url/entity/path/Incidents|Entity Name> "
        "has been resolved by *Actor Name*.\n\n"
        "*Note*: Issue has been resolved successfully.\n\n"
        "*Asset Owners*: <@U12345>, <@U12346>\n"
        "*Impacted Asset Owners*: <@G12347>"
    )

    assert expected_string == text

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 3  # title + note + divider
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_incident_resolved_required_args_only_success(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_resolved_required_args: NotificationRequestClass,
) -> None:
    # Setup mocked identity provider response
    identity_provider.batch_get_actors.return_value = {  # type: ignore
        "owner1": User(urn="owner1", email="owner1@example.com", slack="U12345"),
        "owner2": User(urn="owner2", email="owner2@example.com", slack="U12346"),
        "downstream1": Group(urn="downstream1", slack="G12347"),
    }

    # Call the function
    text, blocks, attachments = build_incident_status_change_message(
        notification_request_resolved_required_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":white_check_mark: *Data Incident Resolved*\n\n"
        "Incident *Incident Alert* on asset <https://base.url/entity/path/Incidents|Entity Name> "
        "has been resolved.\n\n"
        "*Note*: None\n\n"
        "*Asset Owners*: None\n"
        "*Impacted Asset Owners*: None"
    )

    assert text == expected_string

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 3  # title + note + divider
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_incident_reopened_success(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_reopened_all_args: NotificationRequestClass,
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

    text, blocks, attachments = build_incident_status_change_message(
        notification_request_reopened_all_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *Data Incident Reopened*\n\n"
        "Incident *Freshness Assertion <https://base.url/entity/path/Validation/Assertions?assertion_urn=urn:li:assertion:test|Table was not updated in past 6 hours> has failed* on asset <https://base.url/entity/path/Incidents|Entity Name> "
        "has been reopened by *Actor Name*.\n\n"
        "*Asset Owners*: <@U12345>, <@U12346>\n"
        "*Impacted Asset Owners*: <@G12347>"
    )

    assert expected_string == text

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 1
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_incident_reopened_required_args_only(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_reopened_required_args: NotificationRequestClass,
) -> None:
    # Setup mocked identity provider response
    identity_provider.batch_get_actors.return_value = {  # type: ignore
        "owner1": User(urn="owner1", email="owner1@example.com", slack="U12345"),
        "owner2": User(urn="owner2", email="owner2@example.com", slack="U12346"),
        "downstream1": Group(urn="downstream1", slack="G12347"),
    }

    # Call the function
    text, blocks, attachments = build_incident_status_change_message(
        notification_request_reopened_required_args,
        identity_provider,
        mock_client,
        "https://base.url",
    )

    expected_string = (
        ":warning: *Data Incident Reopened*\n\n"
        "Incident *Incident Alert* on asset <https://base.url/entity/path/Incidents|Entity Name> "
        "has been reopened.\n\n"
        "*Asset Owners*: None\n"
        "*Impacted Asset Owners*: None"
    )

    assert text == expected_string

    # Simply verify the blocks and attachments were generated properly.
    assert len(blocks) == 1
    assert len(attachments) == 1
    assert len(attachments[0]["blocks"]) == 5


def test_incident_status_change_failure_identity_provider(
    mock_client: WebClient,
    identity_provider: IdentityProvider,
    notification_request_resolved_all_args: NotificationRequestClass,
) -> None:
    # Setup identity provider to raise an exception
    identity_provider.batch_get_actors.side_effect = Exception("Failed to fetch actors")  # type: ignore

    # Call the function with an expectation of failure
    with patch(
        "datahub_integrations.notifications.sinks.slack.template_utils.logger"
    ) as mock_logger:
        text, _, _ = build_incident_status_change_message(
            notification_request_resolved_all_args,
            identity_provider,
            mock_client,
            "https://base.url",
        )

    # Check logs and response
    mock_logger.exception.assert_called_once_with(
        "Failed to resolve actors from identity provider."
    )
    assert "*Asset Owners*: None" in text
    assert "*Impacted Asset Owners*: None" in text


# type: ignore[attr]
