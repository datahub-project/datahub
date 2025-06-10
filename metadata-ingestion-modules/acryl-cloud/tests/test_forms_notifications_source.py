import json
from typing import cast
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.datahub_forms_notifications.forms_notifications_source import (
    DataHubFormsNotificationsSource,
    DataHubFormsNotificationsSourceConfig,
)
from acryl_datahub_cloud.datahub_forms_notifications.query import (
    GRAPHQL_GET_SEARCH_RESULTS_TOTAL,
    GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST,
)
from acryl_datahub_cloud.notifications.notification_recipient_builder import (
    NotificationRecipientBuilder,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    FormActorAssignmentClass,
    FormInfoClass,
    FormStateClass,
    FormStatusClass,
    FormTypeClass,
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

    mock_graph.get_entities.return_value = {
        "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}
    }

    forms = source.get_forms()
    assert len(forms) == 1
    assert forms[0][0] == "urn:li:form:test-form-1"
    assert forms[0][1] == mock_form_info


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

    # Mock the form info response
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)},
        "urn:li:form:test-form-2": {"formInfo": (mock_form_info, None)},
    }

    forms = source.get_forms()
    assert len(forms) == 2
    assert forms[0][0] == "urn:li:form:test-form-1"
    assert forms[1][0] == "urn:li:form:test-form-2"


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
    source.recipient_builder = mock_recipient_builder

    # Mock successful notification send
    mock_graph.execute_graphql.return_value = {"sendFormNotificationRequest": True}

    source.process_notify_on_publish(["urn:li:corpuser:user1"], "Test Form")

    # Verify the notification was sent with correct parameters
    mock_graph.execute_graphql.assert_called_once()
    call_args = mock_graph.execute_graphql.call_args[1]
    variables = call_args["variables"]

    # Verify notification type and parameters
    assert variables["input"]["type"] == "BROADCAST_COMPLIANCE_FORM_PUBLISH"
    assert variables["input"]["parameters"][0]["value"] == "Test Form"

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

    # Mock form info
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(users=["urn:li:corpuser:test-user"]),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}
    }

    # Mock form completion check
    mock_graph.execute_graphql.side_effect = [
        # First call for form info
        {"data": {"some": "data"}},
        # Second call for is_form_complete
        {"searchAcrossEntities": {"total": 0}},
    ]

    workunits = list(source.get_workunits())
    assert len(workunits) == 0  # This source doesn't produce work units


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
    source.recipient_builder = mock_recipient_builder

    # Mock form info
    mock_form_info = FormInfoClass(
        name="Test Form",
        type=FormTypeClass.COMPLETION,
        status=FormStatusClass(state=FormStateClass.PUBLISHED),
        actors=FormActorAssignmentClass(
            users=["urn:li:corpuser:user1"], groups=["urn:li:corpGroup:group1"]
        ),
    )

    # Mock get_forms response
    mock_graph.get_entities.return_value = {
        "urn:li:form:test-form-1": {"formInfo": (mock_form_info, None)}
    }

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

    # Verify the form was retrieved
    mock_graph.get_entities.assert_called_once_with(
        "form", ["urn:li:form:test-form-1"], ["formInfo"]
    )

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
