import json
import time
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.datahub_forms_notifications.forms_notifications_source import (
    DataHubFormsNotificationsSource,
    DataHubFormsNotificationsSourceConfig,
)
from acryl_datahub_cloud.datahub_forms_notifications.query import (
    GRAPHQL_GET_FEATURE_FLAG,
    GRAPHQL_GET_SEARCH_RESULTS_TOTAL,
    GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST,
)
from acryl_datahub_cloud.notifications.notification_recipient_builder import (
    NotificationRecipientBuilder,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    CorpUserAppearanceSettingsClass,
    CorpUserSettingsClass,
    FormActorAssignmentClass,
    FormInfoClass,
    FormNotificationDetailsClass,
    FormNotificationEntryClass,
    FormNotificationsClass,
    FormNotificationSettingsClass,
    FormSettingsClass,
    FormStateClass,
    FormStatusClass,
    FormTypeClass,
    NotificationSettingClass,
    NotificationSettingsClass,
    NotificationSettingValueClass,
    NotificationSinkTypeClass,
    SlackNotificationSettingsClass,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def mock_context(mock_graph: MagicMock) -> MagicMock:
    ctx = MagicMock(spec=PipelineContext)
    ctx.require_graph.return_value = mock_graph
    return ctx


@pytest.fixture
def basic_config() -> DataHubFormsNotificationsSourceConfig:
    return DataHubFormsNotificationsSourceConfig(form_urns=["urn:li:form:test-form-1"])


@pytest.fixture
def source(
    mock_context: MagicMock, basic_config: DataHubFormsNotificationsSourceConfig
) -> DataHubFormsNotificationsSource:
    source = DataHubFormsNotificationsSource(basic_config, mock_context)
    # Initialize the graph client and ensure it's typed as a MagicMock
    source.graph = cast(DataHubGraph, mock_context.require_graph())
    assert source.graph is not None  # For mypy
    return source


def test_init(
    source: DataHubFormsNotificationsSource,
    mock_context: MagicMock,
    basic_config: DataHubFormsNotificationsSourceConfig,
) -> None:
    """Test basic initialization of the source"""
    assert source.config == basic_config
    assert source.ctx == mock_context
    assert source.report is not None
    assert source.report.notifications_sent == 0


def test_execute_graphql_with_retry_success(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test successful GraphQL execution"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    expected_response = {"data": {"some": "data"}}
    mock_graph.execute_graphql.return_value = expected_response

    response = source.execute_graphql_with_retry("query {}", {})

    assert response == expected_response
    mock_graph.execute_graphql.assert_called_once_with("query {}", variables={})


def test_execute_graphql_with_retry_error(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test GraphQL execution with error"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    mock_graph.execute_graphql.return_value = {"error": "Something went wrong"}

    with pytest.raises(Exception, match="GraphQL error: Something went wrong"):
        source.execute_graphql_with_retry("query {}", {})


def test_get_forms_with_config_urns(source: DataHubFormsNotificationsSource) -> None:
    """Test getting forms from config URNs"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_form_settings = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )

    # Mock get_entities to return different responses based on the call
    def mock_get_entities(
        entity_type: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Any]:
        if "formSettings" in aspects:
            return {
                "urn:li:form:test-form-1": {"formSettings": (mock_form_settings, None)}
            }
        elif "formInfo" in aspects:
            return {"urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}}
        return {}

    mock_graph.get_entities.side_effect = mock_get_entities

    forms = source.get_forms()
    assert len(forms) == 1
    assert forms[0][0] == "urn:li:form:test-form-1"
    assert forms[0][1] == mock_form_info

    # Verify both formSettings and formInfo were fetched
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formSettings"]
    )
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formInfo"]
    )


def test_get_forms_with_search(source: DataHubFormsNotificationsSource) -> None:
    """Test getting forms through search when no URNs provided"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Override config to have no form URNs
    source.config.form_urns = None

    # Mock the search response
    mock_graph.execute_graphql.return_value = {
        "scrollAcrossEntities": {
            "nextScrollId": None,
            "searchResults": [
                {"entity": {"urn": "urn:li:form:test-form-1"}},
                {"entity": {"urn": "urn:li:form:test-form-2"}},
            ],
        }
    }

    # Mock form info and settings
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_form_settings = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )

    # Mock get_entities to return different responses based on the call
    def mock_get_entities(
        entity_type: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Any]:
        if "formSettings" in aspects:
            return {
                "urn:li:form:test-form-1": {"formSettings": (mock_form_settings, None)},
                "urn:li:form:test-form-2": {"formSettings": (mock_form_settings, None)},
            }
        elif "formInfo" in aspects:
            return {
                "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)},
                "urn:li:form:test-form-2": {"formInfo": (mock_form_info, None)},
            }
        return {}

    mock_graph.get_entities.side_effect = mock_get_entities

    forms = source.get_forms()
    assert len(forms) == 2
    assert forms[0][0] == "urn:li:form:test-form-1"
    assert forms[1][0] == "urn:li:form:test-form-2"

    # Verify both formSettings and formInfo were fetched
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1", "urn:li:form:test-form-2"], ["formSettings"]
    )
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1", "urn:li:form:test-form-2"], ["formInfo"]
    )


def test_get_form_assignees(source: DataHubFormsNotificationsSource) -> None:
    """Test getting form assignees including users and group members"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    form = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(
            users=["urn:li:corpuser:user1"], groups=["urn:li:corpGroup:group1"]
        ),
    )

    # Mock group members
    mock_graph.get_related_entities.return_value = [
        MagicMock(urn="urn:li:corpuser:group-user1"),
        MagicMock(urn="urn:li:corpuser:group-user2"),
    ]

    assignees = source.get_form_assignees("urn:li:form:test-form-1", form)
    assert len(assignees) == 3
    assert "urn:li:corpuser:user1" in assignees
    assert "urn:li:corpuser:group-user1" in assignees
    assert "urn:li:corpuser:group-user2" in assignees


def test_is_form_complete(source: DataHubFormsNotificationsSource) -> None:
    """Test checking if a form is complete"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Test complete form
    mock_graph.execute_graphql.return_value = {"searchAcrossEntities": {"total": 0}}

    assert (
        source.is_form_complete("urn:li:form:test-form-1", FormTypeClass.COMPLETION)
        is True
    )

    # Test incomplete form
    mock_graph.execute_graphql.return_value = {"searchAcrossEntities": {"total": 5}}

    assert (
        source.is_form_complete("urn:li:form:test-form-1", FormTypeClass.COMPLETION)
        is False
    )


def test_process_notify_on_publish(source: DataHubFormsNotificationsSource) -> None:
    """Test processing notifications for form publish"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock the recipient builder
    mock_recipient_builder = MagicMock(spec=NotificationRecipientBuilder)
    mock_recipient_builder.build_actor_recipients.return_value = [
        {
            "actor": "urn:li:corpuser:user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        }
    ]
    mock_recipient_builder.convert_recipients_to_json_objects.return_value = [
        {
            "actor": "urn:li:corpuser:user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        }
    ]
    source.recipient_builder = mock_recipient_builder

    # Mock successful notification send
    mock_graph.execute_graphql.return_value = {"sendFormNotificationRequest": True}

    source.process_notify_on_publish(
        ["urn:li:corpuser:user1"],
        "Test Form",
        "urn:li:form:test-form-1",
        "test description",
    )

    # Verify the notification was sent with correct parameters
    mock_graph.execute_graphql.assert_called_once()
    call_args = mock_graph.execute_graphql.call_args[1]
    variables = call_args["variables"]

    # Verify notification type and parameters
    assert variables["input"]["type"] == "BROADCAST_COMPLIANCE_FORM_PUBLISH"
    assert variables["input"]["parameters"][0]["value"] == "Test Form"
    assert variables["input"]["parameters"][1]["value"] == "test description"

    # Verify recipients structure
    recipients = variables["input"]["recipients"]
    assert isinstance(recipients, list)
    assert len(recipients) == 1  # One recipient for one user

    # Verify recipient structure
    recipient = recipients[0]
    assert "actor" in recipient
    assert recipient["actor"] == "urn:li:corpuser:user1"
    assert "type" in recipient
    assert recipient["type"] == "COMPLIANCE_FORM_PUBLISH"
    assert "parameters" in recipient
    assert isinstance(recipient["parameters"], list)

    # Verify the recipient builder was called correctly
    mock_recipient_builder.build_actor_recipients.assert_called_once_with(
        ["urn:li:corpuser:user1"], "COMPLIANCE_FORM_PUBLISH", True
    )


def test_get_workunits(source: DataHubFormsNotificationsSource) -> None:
    """Test getting workunits from the source"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock form info and settings
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_form_settings = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )

    # Mock get_entities to return different responses based on the call
    def mock_get_entities(
        entity_type: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Any]:
        if "formSettings" in aspects:
            return {
                "urn:li:form:test-form-1": {"formSettings": (mock_form_settings, None)}
            }
        elif "formInfo" in aspects:
            return {"urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}}
        return {}

    mock_graph.get_entities.side_effect = mock_get_entities

    # Mock form completion check
    mock_graph.execute_graphql.side_effect = [
        # First call for feature flag check
        {"appConfig": {"featureFlags": {"formsNotificationsEnabled": True}}},
        # Second call for is_form_complete
        {"searchAcrossEntities": {"total": 0}},
    ]

    workunits = list(source.get_workunits())
    assert len(workunits) == 0  # This source doesn't produce work units

    # Verify both formSettings and formInfo were fetched
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formSettings"]
    )
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formInfo"]
    )


def test_get_incomplete_assets_for_form(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test getting the correct filter based on form type"""
    # Test completion form filter
    completion_filter = source._get_incomplete_assets_for_form(
        "urn:li:form:test-form-1", FormTypeClass.COMPLETION
    )
    assert len(completion_filter) == 1
    assert completion_filter[0]["and"][0]["field"] == "incompleteForms"
    assert completion_filter[0]["and"][0]["values"] == ["urn:li:form:test-form-1"]

    # Test verification form filter
    verification_filter = source._get_incomplete_assets_for_form(
        "urn:li:form:test-form-1", FormTypeClass.VERIFICATION
    )
    assert len(verification_filter) == 2

    # Check first condition (incomplete forms)
    assert verification_filter[0]["and"][0]["field"] == "incompleteForms"
    assert verification_filter[0]["and"][0]["values"] == ["urn:li:form:test-form-1"]

    # Check second condition (completed but not verified)
    assert verification_filter[1]["and"][0]["field"] == "completedForms"
    assert verification_filter[1]["and"][0]["values"] == ["urn:li:form:test-form-1"]
    assert verification_filter[1]["and"][1]["field"] == "verifiedForms"
    assert verification_filter[1]["and"][1]["values"] == ["urn:li:form:test-form-1"]
    assert verification_filter[1]["and"][1]["negated"] is True


def test_notify_form_assignees(source: DataHubFormsNotificationsSource) -> None:
    """Test the complete flow of notifying form assignees"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock the recipient builder
    mock_recipient_builder = MagicMock(spec=NotificationRecipientBuilder)
    mock_recipient_builder.build_actor_recipients.return_value = [
        {
            "actor": "urn:li:corpuser:user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        },
        {
            "actor": "urn:li:corpuser:group-user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        },
    ]
    mock_recipient_builder.convert_recipients_to_json_objects.return_value = [
        {
            "actor": "urn:li:corpuser:user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        },
        {
            "actor": "urn:li:corpuser:group-user1",
            "type": "COMPLIANCE_FORM_PUBLISH",
            "parameters": [],
        },
    ]
    source.recipient_builder = mock_recipient_builder

    # Mock form info and settings
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(
            users=["urn:li:corpuser:user1"], groups=["urn:li:corpGroup:group1"]
        ),
    )

    mock_form_settings = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )

    # Mock form notifications (empty for all users)
    mock_form_notifications = FormNotificationsClass(notificationDetails=[])

    # Mock get_entities to return different responses based on the call
    def mock_get_entities(
        entity_type: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Any]:
        if entity_type == "form":
            if "formSettings" in aspects:
                return {
                    "urn:li:form:test-form-1": {
                        "formSettings": (mock_form_settings, None)
                    }
                }
            elif "formInfo" in aspects:
                return {"urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}}
        elif entity_type == "corpuser":
            return {
                urn: {"formNotifications": (mock_form_notifications, None)}
                for urn in urns
            }
        return {}

    mock_graph.get_entities.side_effect = mock_get_entities

    # Mock group members
    mock_graph.get_related_entities.return_value = [
        MagicMock(urn="urn:li:corpuser:group-user1"),
        MagicMock(urn="urn:li:corpuser:group-user2"),
    ]

    # Mock form completion check
    mock_graph.execute_graphql.side_effect = [
        # First call for is_form_complete
        {"searchAcrossEntities": {"total": 5}},  # Form is incomplete
        # Second call for sending notification
        {"sendFormNotificationRequest": True},
    ]

    # Execute the notification flow
    source.notify_form_assignees()

    # Verify the form was retrieved with both aspects
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formSettings"]
    )
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formInfo"]
    )

    # Verify form notifications were retrieved for all users
    # Get all calls to get_entities
    get_entities_calls = [
        call
        for call in mock_graph.get_entities.call_args_list
        if call[0][0] == "corpuser"  # Filter for corpuser calls
    ]
    assert len(get_entities_calls) == 1  # Should be exactly one call for corpuser

    # Get the actual URNs from the call
    actual_urns = get_entities_calls[0][0][1]  # Get the URNs list from the call
    expected_urns = [
        "urn:li:corpuser:user1",
        "urn:li:corpuser:group-user1",
        "urn:li:corpuser:group-user2",
    ]

    # Compare sorted lists
    assert sorted(actual_urns) == sorted(expected_urns)
    assert get_entities_calls[0][0][2] == ["formNotifications"]  # Verify aspects

    # Verify form completion was checked
    mock_graph.execute_graphql.assert_any_call(
        GRAPHQL_GET_SEARCH_RESULTS_TOTAL,
        variables={
            "count": 0,
            "orFilters": source._get_incomplete_assets_for_form(
                "urn:li:form:test-form-1", FormTypeClass.COMPLETION
            ),
        },
    )

    # Verify group members were fetched
    mock_graph.get_related_entities.assert_called_once_with(
        "urn:li:corpGroup:group1",
        ["IsMemberOfGroup", "IsMemberOfNativeGroup"],
        mock_graph.RelationshipDirection.INCOMING,
    )

    # Verify notification was sent
    mock_graph.execute_graphql.assert_any_call(
        GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST,
        variables={
            "input": {
                "type": "BROADCAST_COMPLIANCE_FORM_PUBLISH",
                "parameters": [{"key": "formName", "value": "Test Form"}],
                "recipients": mock_recipient_builder.build_actor_recipients.return_value,
            }
        },
    )

    # Verify recipient builder was called with all assignees
    expected_assignees = [
        "urn:li:corpuser:user1",
        "urn:li:corpuser:group-user1",
        "urn:li:corpuser:group-user2",
    ]
    actual_call = mock_recipient_builder.build_actor_recipients.call_args[0]
    assert sorted(actual_call[0]) == sorted(
        expected_assignees
    )  # Sort both lists before comparison
    assert actual_call[1] == "COMPLIANCE_FORM_PUBLISH"
    assert actual_call[2] is True

    # Verify report counters
    assert source.report.notifications_sent == 2  # Two recipients in the mock response
    assert source.report.forms_count == 1  # One form processed


def test_notify_form_assignees_no_recipients(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test that report counters are not updated when there are no recipients"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock the recipient builder to return no recipients
    mock_recipient_builder = MagicMock(spec=NotificationRecipientBuilder)
    mock_recipient_builder.build_actor_recipients.return_value = []
    source.recipient_builder = mock_recipient_builder

    # Mock form info
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=[]),  # No users or groups
    )

    # Mock get_forms response
    mock_graph.get_entities.return_value = {
        "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}
    }

    # Mock form completion check
    mock_graph.execute_graphql.return_value = {
        "searchAcrossEntities": {"total": 5}  # Form is incomplete
    }

    # Execute the notification flow
    source.notify_form_assignees()

    # Verify report counters are not updated
    assert source.report.notifications_sent == 0
    assert source.report.forms_count == 0


def test_get_form_assignees_with_owners(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test getting form assignees including owners from assets"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock form info with owners flag set to True
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(
            users=["urn:li:corpuser:user1"],
            groups=["urn:li:corpGroup:group1"],
            owners=True,  # Enable owner-based assignees
        ),
    )

    # Mock group members
    mock_graph.get_related_entities.return_value = [
        MagicMock(urn="urn:li:corpuser:group-user1"),
        MagicMock(urn="urn:li:corpuser:group-user2"),
    ]

    # Mock asset search results with owners
    mock_graph.get_results_by_filter.return_value = [
        {
            "extraProperties": [
                {"name": "urn", "value": json.dumps("urn:li:dataset:asset1")},
                {
                    "name": "owners",
                    "value": json.dumps(
                        ["urn:li:corpuser:owner1", "urn:li:corpGroup:owner-group1"]
                    ),
                },
            ]
        },
        {
            "extraProperties": [
                {"name": "urn", "value": json.dumps("urn:li:dataset:asset2")},
                {
                    "name": "owners",
                    "value": json.dumps(
                        ["urn:li:corpuser:owner2", "urn:li:corpuser:owner3"]
                    ),
                },
            ]
        },
    ]

    # Get assignees
    assignees = source.get_form_assignees("urn:li:form:test-form-1", mock_form_info)

    # Verify all types of assignees are included
    expected_assignees = [
        # Direct users from form
        "urn:li:corpuser:user1",
        # Group members
        "urn:li:corpuser:group-user1",
        "urn:li:corpuser:group-user2",
        # Asset owners
        "urn:li:corpuser:owner1",
        "urn:li:corpuser:owner2",
        "urn:li:corpuser:owner3",
    ]

    # Sort both lists for comparison since order doesn't matter
    assert sorted(assignees) == sorted(expected_assignees)

    # Verify the correct filter was used to get assets
    mock_graph.get_results_by_filter.assert_called_once()
    call_args = mock_graph.get_results_by_filter.call_args[1]
    assert call_args["extra_or_filters"] == source._get_incomplete_assets_for_form(
        "urn:li:form:test-form-1", FormTypeClass.COMPLETION
    )
    assert call_args["skip_cache"] is True


def test_filter_assignees_to_notify(source: DataHubFormsNotificationsSource) -> None:
    """Test filtering assignees who haven't been notified yet"""
    mock_graph = cast(MagicMock, source.graph)

    # Mock user settings with notifications enabled
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"slack.enabled": "true"},
                )
            },
            slackSettings=SlackNotificationSettingsClass(userHandle="test-user"),
        ),
    )

    # Mock form notifications for one user
    mock_form_notifications = FormNotificationsClass(
        notificationDetails=[
            FormNotificationDetailsClass(
                formUrn="urn:li:form:test-form",
                notificationLog=[
                    FormNotificationEntryClass(
                        time=int(time.time() * 1000),
                        notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
                    )
                ],
            )
        ]
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {
            "corpUserSettings": (mock_settings, None),
            "formNotifications": (mock_form_notifications, None),
        },
        "urn:li:corpuser:test-user-2": {
            "corpUserSettings": (mock_settings, None),
            "formNotifications": (None, None),  # No notifications yet
        },
    }

    # Test filtering users for a form they haven't been notified about
    filtered_users = source.filter_assignees_to_notify(
        ["urn:li:corpuser:test-user", "urn:li:corpuser:test-user-2"],
        "urn:li:form:different-form",
    )

    assert (
        len(filtered_users) == 2
    )  # Both users should be included for a different form

    # Test filtering users for a form they have been notified about
    filtered_users = source.filter_assignees_to_notify(
        ["urn:li:corpuser:test-user", "urn:li:corpuser:test-user-2"],
        "urn:li:form:test-form",
    )

    assert len(filtered_users) == 1  # Only test-user-2 should be included
    assert filtered_users[0] == "urn:li:corpuser:test-user-2"


def test_has_user_been_sent_notification(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test checking if a user has been sent a notification"""
    mock_form_notifications = FormNotificationsClass(
        notificationDetails=[
            FormNotificationDetailsClass(
                formUrn="urn:li:form:test-form",
                notificationLog=[
                    FormNotificationEntryClass(
                        time=int(time.time() * 1000),
                        notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
                    )
                ],
            )
        ]
    )

    # Test user who has been notified
    assert (
        source.has_user_been_sent_notification(
            "urn:li:corpuser:test-user",
            "urn:li:form:test-form",
            mock_form_notifications,
            "BROADCAST_COMPLIANCE_FORM_PUBLISH",
        )
        is True
    )

    # Test user who hasn't been notified for this form
    assert (
        source.has_user_been_sent_notification(
            "urn:li:corpuser:test-user",
            "urn:li:form:different-form",
            mock_form_notifications,
            "BROADCAST_COMPLIANCE_FORM_PUBLISH",
        )
        is False
    )

    # Test user who hasn't been notified for this notification type
    assert (
        source.has_user_been_sent_notification(
            "urn:li:corpuser:test-user",
            "urn:li:form:test-form",
            mock_form_notifications,
            "DIFFERENT_NOTIFICATION_TYPE",
        )
        is False
    )


def test_get_notification_details_for_form(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test getting notification details for a specific form"""
    mock_form_notifications = FormNotificationsClass(
        notificationDetails=[
            FormNotificationDetailsClass(
                formUrn="urn:li:form:test-form",
                notificationLog=[
                    FormNotificationEntryClass(
                        time=int(time.time() * 1000),
                        notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
                    )
                ],
            )
        ]
    )

    # Test getting details for existing form
    details = source.get_notification_details_for_form(
        "urn:li:corpuser:test-user",
        "urn:li:form:test-form",
        mock_form_notifications,
    )
    assert details is not None
    assert details.formUrn == "urn:li:form:test-form"

    # Test getting details for non-existent form
    details = source.get_notification_details_for_form(
        "urn:li:corpuser:test-user",
        "urn:li:form:different-form",
        mock_form_notifications,
    )
    assert details is None


def test_populate_user_to_form_notifications(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test populating user form notifications map"""
    mock_graph = cast(MagicMock, source.graph)

    mock_form_notifications = FormNotificationsClass(
        notificationDetails=[
            FormNotificationDetailsClass(
                formUrn="urn:li:form:test-form",
                notificationLog=[
                    FormNotificationEntryClass(
                        time=int(time.time() * 1000),
                        notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
                    )
                ],
            )
        ]
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {
            "formNotifications": (mock_form_notifications, None),
        },
        "urn:li:corpuser:test-user-2": {
            "formNotifications": (None, None),
        },
    }

    # Test populating map for new users
    source.populate_user_to_form_notifications(
        ["urn:li:corpuser:test-user", "urn:li:corpuser:test-user-2"]
    )

    assert "urn:li:corpuser:test-user" in source.user_to_form_notifications
    assert (
        source.user_to_form_notifications["urn:li:corpuser:test-user"]
        == mock_form_notifications
    )
    assert "urn:li:corpuser:test-user-2" not in source.user_to_form_notifications

    # Test that existing users aren't re-fetched
    mock_graph.get_entities.reset_mock()
    source.populate_user_to_form_notifications(["urn:li:corpuser:test-user"])
    mock_graph.get_entities.assert_not_called()


def test_update_form_notifications(source: DataHubFormsNotificationsSource) -> None:
    """Test updating form notifications for a user"""
    mock_graph = cast(MagicMock, source.graph)

    # Initial form notifications
    initial_notifications = FormNotificationsClass(
        notificationDetails=[
            FormNotificationDetailsClass(
                formUrn="urn:li:form:existing-form",
                notificationLog=[
                    FormNotificationEntryClass(
                        time=int(time.time() * 1000),
                        notificationType="BROADCAST_COMPLIANCE_FORM_PUBLISH",
                    )
                ],
            )
        ]
    )

    source.user_to_form_notifications["urn:li:corpuser:test-user"] = (
        initial_notifications
    )

    # Update notifications for a new form
    source.update_form_notifications(
        "urn:li:corpuser:test-user",
        "urn:li:form:new-form",
    )

    # Verify that the graph client was called to emit the updated notifications
    mock_graph.emit.assert_called_once()
    emitted_wrapper = mock_graph.emit.call_args[0][0]
    assert emitted_wrapper.entityUrn == "urn:li:corpuser:test-user"
    assert isinstance(emitted_wrapper.aspect, FormNotificationsClass)

    # Verify the updated notifications contain both forms
    updated_notifications = emitted_wrapper.aspect
    assert len(updated_notifications.notificationDetails) == 2

    # Verify the new form notification was added
    new_form_details = next(
        d
        for d in updated_notifications.notificationDetails
        if d.formUrn == "urn:li:form:new-form"
    )
    assert len(new_form_details.notificationLog) == 1
    assert (
        new_form_details.notificationLog[0].notificationType
        == "BROADCAST_COMPLIANCE_FORM_PUBLISH"
    )


def test_is_feature_flag_enabled(source: DataHubFormsNotificationsSource) -> None:
    """Test checking if the feature flag is enabled"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Test when feature flag is enabled
    mock_graph.execute_graphql.return_value = {
        "appConfig": {"featureFlags": {"formsNotificationsEnabled": True}}
    }
    assert source.is_feature_flag_enabled() is True

    # Test when feature flag is disabled
    mock_graph.execute_graphql.return_value = {
        "appConfig": {"featureFlags": {"formsNotificationsEnabled": False}}
    }
    assert source.is_feature_flag_enabled() is False

    # Test when feature flag is not present
    mock_graph.execute_graphql.return_value = {"appConfig": {"featureFlags": {}}}
    assert source.is_feature_flag_enabled() is False

    # Test when appConfig is not present
    mock_graph.execute_graphql.return_value = {}
    assert source.is_feature_flag_enabled() is False


def test_get_workunits_feature_flag_disabled(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test getting workunits when feature flag is disabled"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock feature flag check to return disabled
    mock_graph.execute_graphql.return_value = {
        "appConfig": {"featureFlags": {"formsNotificationsEnabled": False}}
    }

    workunits = list(source.get_workunits())
    assert len(workunits) == 0  # Should return empty list when feature flag is disabled

    # Verify feature flag was checked
    mock_graph.execute_graphql.assert_called_once_with(
        GRAPHQL_GET_FEATURE_FLAG, variables={}
    )

    # Verify no other methods were called
    mock_graph.get_entities.assert_not_called()


def test_get_workunits_feature_flag_enabled(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test getting workunits when feature flag is enabled"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock form info and settings
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_form_settings = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )

    # Mock get_entities to return different responses based on the call
    def mock_get_entities(
        entity_type: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Any]:
        if "formSettings" in aspects:
            return {
                "urn:li:form:test-form-1": {"formSettings": (mock_form_settings, None)}
            }
        elif "formInfo" in aspects:
            return {"urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}}
        return {}

    mock_graph.get_entities.side_effect = mock_get_entities

    # Mock feature flag check to return enabled
    mock_graph.execute_graphql.side_effect = [
        # First call for feature flag check
        {"appConfig": {"featureFlags": {"formsNotificationsEnabled": True}}},
        # Second call for is_form_complete
        {"searchAcrossEntities": {"total": 0}},
    ]

    workunits = list(source.get_workunits())
    assert len(workunits) == 0  # This source doesn't produce work units

    # Verify feature flag was checked
    mock_graph.execute_graphql.assert_any_call(GRAPHQL_GET_FEATURE_FLAG, variables={})

    # Verify both formSettings and formInfo were fetched
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formSettings"]
    )
    mock_graph.get_entities.assert_any_call(
        "form", ["urn:li:form:test-form-1"], ["formInfo"]
    )


def test_get_form_urns_with_notifications_enabled(
    source: DataHubFormsNotificationsSource,
) -> None:
    """Test filtering form URNs based on notification settings"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock form settings with different notification configurations
    mock_form_settings_enabled = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=True
        )
    )
    mock_form_settings_disabled = FormSettingsClass(
        notificationSettings=FormNotificationSettingsClass(
            notifyAssigneesOnPublish=False
        )
    )

    # Mock get_entities to return different form settings
    mock_graph.get_entities.return_value = {
        "urn:li:form:form1": {"formSettings": (mock_form_settings_enabled, None)},
        "urn:li:form:form2": {"formSettings": (mock_form_settings_disabled, None)},
        "urn:li:form:form3": {"formSettings": (mock_form_settings_enabled, None)},
        "urn:li:form:form4": {"formSettings": (None, None)},  # No settings
    }

    # Test with multiple form URNs
    form_urns = [
        "urn:li:form:form1",
        "urn:li:form:form2",
        "urn:li:form:form3",
        "urn:li:form:form4",
    ]
    filtered_urns = source.get_form_urns_with_notifications_enabled(form_urns)

    # Verify the correct URNs were returned
    assert len(filtered_urns) == 2
    assert "urn:li:form:form1" in filtered_urns
    assert "urn:li:form:form3" in filtered_urns
    assert "urn:li:form:form2" not in filtered_urns  # Notifications disabled
    assert "urn:li:form:form4" not in filtered_urns  # No settings

    # Verify get_entities was called correctly
    mock_graph.get_entities.assert_called_once_with("form", form_urns, ["formSettings"])

    # Test with empty list
    mock_graph.get_entities.reset_mock()
    filtered_urns = source.get_form_urns_with_notifications_enabled([])
    assert len(filtered_urns) == 0
    mock_graph.get_entities.assert_not_called()
