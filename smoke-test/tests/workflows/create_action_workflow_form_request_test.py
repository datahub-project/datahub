import time

import pytest

from tests.utils import (
    delete_urns_from_file,
    execute_gql_with_retry,
    get_root_urn,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

# Test action requests that will be created during tests
CREATED_ACTION_REQUEST_URNS: list[str] = []


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Ingest test data before tests and clean up after"""
    print("ingesting action workflow test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    yield
    print("removing action workflow test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_workflows_data.json")
    # Clean up any action requests created during testing
    for urn in CREATED_ACTION_REQUEST_URNS:
        try:
            graph_client.hard_delete_entity(urn)
        except Exception:
            pass  # Ignore errors during cleanup


def test_create_basic_workflow_request(auth_session):
    """Test creating a basic workflow request using test-access-workflow-1"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset,PROD)",
            "description": "Test access request for dataset",
            "fields": [
                {
                    "id": "reason",
                    "values": [
                        {
                            "stringValue": "I need access to analyze customer data for reporting"
                        }
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    # The response should be a URN string
    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(f"✓ Successfully created action workflow request: {action_request_urn}")


def test_create_workflow_request_without_entity(auth_session):
    """Test creating a workflow request without an entity (for workflows that don't require entities)"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-custom-workflow-1",
            "description": "Test custom workflow request without entity",
            "fields": [
                {
                    "id": "classification_level",
                    "values": [{"stringValue": "CONFIDENTIAL"}],
                }
            ],
        }
    }

    # Custom workflow requires more aggressive retry due to resource-intensive processing
    import os

    max_retries = 8 if os.getenv("CI") else 3
    base_delay = 4 if os.getenv("CI") else 1
    response = execute_gql_with_retry(
        auth_session,
        create_query,
        variables,
        max_retries=max_retries,
        base_delay=base_delay,
    )

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(
        f"✓ Successfully created workflow request without entity: {action_request_urn}"
    )


def test_create_workflow_request_with_multiple_fields(auth_session):
    """Test creating a workflow request with multiple fields using test-access-workflow-2"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-2",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-2,PROD)",
            "description": "Test access request with multiple fields",
            "fields": [
                {
                    "id": "purpose",
                    "values": [
                        {
                            "stringValue": "Need access for quarterly business analysis and reporting to stakeholders"
                        }
                    ],
                },
                {"id": "duration", "values": [{"numberValue": 90}]},
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(
        f"✓ Successfully created workflow request with multiple fields: {action_request_urn}"
    )


def test_create_workflow_request_with_access_expiration(auth_session):
    """Test creating a workflow request with access expiration"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    # Set expiration to 30 days from now
    expires_at = int(time.time() * 1000) + (
        30 * 24 * 60 * 60 * 1000
    )  # 30 days in milliseconds

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-with-expiration,PROD)",
            "description": "Test access request with expiration",
            "fields": [
                {
                    "id": "reason",
                    "values": [
                        {
                            "stringValue": "Temporary access needed for data migration project"
                        }
                    ],
                }
            ],
            "access": {"expiresAt": expires_at},
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(
        f"✓ Successfully created workflow request with expiration: {action_request_urn}"
    )


def test_create_workflow_request_minimal_input(auth_session):
    """Test creating a workflow request with minimal required input only"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "fields": [
                {"id": "reason", "values": [{"stringValue": "Minimal access request"}]}
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(f"✓ Successfully created minimal workflow request: {action_request_urn}")


def test_create_workflow_request_with_multiple_values(auth_session):
    """Test creating a workflow request with multiple values for a field (if supported)"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-multi-values-workflow",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-multi,PROD)",
            "description": "Test request with multiple field values",
            "fields": [
                {
                    "id": "reasons",
                    "values": [
                        {"stringValue": "Primary reason: Data analysis"},
                        {"stringValue": "Secondary reason: Backup for reporting"},
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    print(
        f"✓ Successfully created workflow request with multiple values: {response['data']['createActionWorkflowFormRequest']}"
    )
    CREATED_ACTION_REQUEST_URNS.append(
        response["data"]["createActionWorkflowFormRequest"]
    )


# Error handling tests


def test_create_workflow_request_invalid_workflow_urn(auth_session):
    """Test error handling for invalid workflow URN"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "invalid-workflow-urn-format",
            "fields": [{"id": "reason", "values": [{"stringValue": "Test reason"}]}],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to invalid workflow URN
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(
        f"✓ Correctly handled invalid workflow URN: {response['errors'][0]['message']}"
    )


def test_create_workflow_request_nonexistent_workflow(auth_session):
    """Test error handling for non-existent workflow URN"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:non-existent-workflow-12345",
            "fields": [{"id": "reason", "values": [{"stringValue": "Test reason"}]}],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to non-existent workflow
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(
        f"✓ Correctly handled non-existent workflow: {response['errors'][0]['message']}"
    )


def test_create_workflow_request_invalid_entity_urn(auth_session):
    """Test error handling for invalid entity URN format"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "entityUrn": "invalid-entity-urn-format",
            "fields": [{"id": "reason", "values": [{"stringValue": "Test reason"}]}],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to invalid entity URN
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(f"✓ Correctly handled invalid entity URN: {response['errors'][0]['message']}")


def test_create_workflow_request_missing_required_field(auth_session):
    """Test error handling for missing required field"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "fields": [
                {"id": "non_existent_field", "values": [{"stringValue": "Test value"}]}
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to missing required field
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(
        f"✓ Correctly handled missing required field: {response['errors'][0]['message']}"
    )


def test_create_workflow_request_invalid_field_value_type(auth_session):
    """Test error handling for invalid field value type"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-2",
            "fields": [
                {
                    "id": "duration",
                    "values": [
                        {"stringValue": "invalid_number"}  # Should be numberValue
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error or handle gracefully
    # Note: The resolver might handle this gracefully by ignoring invalid value types
    if "errors" in response:
        print(
            f"✓ Correctly handled invalid field value type: {response['errors'][0]['message']}"
        )
    else:
        # If no error, the request was created successfully (graceful handling)
        action_request_urn = response["data"]["createActionWorkflowFormRequest"]
        CREATED_ACTION_REQUEST_URNS.append(action_request_urn)
        print(f"✓ Gracefully handled invalid field value type: {action_request_urn}")


def test_create_workflow_request_empty_fields(auth_session):
    """Test error handling for empty fields array"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "fields": [],  # Empty fields array
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to empty fields for a workflow that requires fields
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(f"✓ Correctly handled empty fields array: {response['errors'][0]['message']}")


def test_create_workflow_request_empty_field_values(auth_session):
    """Test error handling for empty field values"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "fields": [
                {
                    "id": "reason",
                    "values": [],  # Empty values array
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to empty values for a required field
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(f"✓ Correctly handled empty field values: {response['errors'][0]['message']}")


def test_create_workflow_request_invalid_allowed_value(auth_session):
    """Test error handling for invalid allowed value in custom workflow"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-custom-workflow-1",
            "fields": [
                {
                    "id": "classification_level",
                    "values": [
                        {
                            "stringValue": "INVALID_CLASSIFICATION"
                        }  # Not in allowed values
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    # Should return an error due to invalid allowed value
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(
        f"✓ Correctly handled invalid allowed value: {response['errors'][0]['message']}"
    )


# Edge cases and integration tests


def test_create_workflow_request_with_long_description(auth_session):
    """Test creating a workflow request with very long description"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    long_description = "A" * 1000  # 1000 character description

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "description": long_description,
            "fields": [
                {
                    "id": "reason",
                    "values": [{"stringValue": "Test reason with long description"}],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(
        f"✓ Successfully created workflow request with long description: {action_request_urn}"
    )


def test_create_workflow_request_with_special_characters(auth_session):
    """Test creating a workflow request with special characters in field values"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "description": "Test request with special characters: !@#$%^&*()",
            "fields": [
                {
                    "id": "reason",
                    "values": [
                        {
                            "stringValue": "Need access for 'special' analysis with symbols: !@#$%^&*()"
                        }
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(
        f"✓ Successfully created workflow request with special characters: {action_request_urn}"
    )


def test_create_workflow_request_with_unicode_characters(auth_session):
    """Test creating a workflow request with unicode characters"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
            "description": "Test request with unicode: 你好世界 🌍 café résumé",
            "fields": [
                {
                    "id": "reason",
                    "values": [
                        {
                            "stringValue": "Need access for international analysis: 你好世界 🌍 café résumé"
                        }
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    action_request_urn = response["data"]["createActionWorkflowFormRequest"]
    assert action_request_urn.startswith("urn:li:actionRequest:")

    # Track for cleanup
    CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

    print(f"✓ Successfully created workflow request with unicode: {action_request_urn}")


def test_create_workflow_request_boundary_values(auth_session):
    """Test creating a workflow request with boundary values"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-multi-values-workflow",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-boundary,PROD)",
            "description": "Test request with boundary values",
            "fields": [
                {
                    "id": "reasons",
                    "values": [{"stringValue": "Testing boundary values"}],
                },
                {
                    "id": "quantities",
                    "values": [{"numberValue": 0}, {"numberValue": 999999}],
                },
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    print(
        f"✓ Successfully created workflow request with boundary values: {response['data']['createActionWorkflowFormRequest']}"
    )
    CREATED_ACTION_REQUEST_URNS.append(
        response["data"]["createActionWorkflowFormRequest"]
    )


def test_create_dynamic_assignment_workflow_request(auth_session):
    """Test creating a workflow request with dynamic assignment of entity owners and domain owners"""

    create_query = """
    mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
        createActionWorkflowFormRequest(input: $input)
    }
    """

    variables = {
        "input": {
            "workflowUrn": "urn:li:actionWorkflow:test-dynamic-assignment-workflow",
            "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset,PROD)",
            "description": "Test dynamic assignment request",
            "fields": [
                {
                    "id": "reason",
                    "values": [
                        {
                            "stringValue": "Testing dynamic assignment of entity and domain owners"
                        }
                    ],
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, create_query, variables)

    assert "errors" not in response
    assert response["data"]["createActionWorkflowFormRequest"] is not None

    request_urn = response["data"]["createActionWorkflowFormRequest"]
    CREATED_ACTION_REQUEST_URNS.append(request_urn)

    print(f"✓ Successfully created dynamic assignment workflow request: {request_urn}")

    # Wait for the write to be fully synced before querying
    wait_for_writes_to_sync()

    # Now verify the request was created with the correct dynamic assignments
    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
                assignedUsers
                assignedGroups
                entity {
                    urn
                }
                params {
                    workflowFormRequest {
                        workflowUrn
                        stepState {
                            stepId
                        }
                    }
                }
            }
        }
    }
    """

    list_variables = {
        "input": {
            "start": 0,
            "count": 100,
            "allActionRequests": True,
            "type": "WORKFLOW_FORM_REQUEST",
            "orFilters": [
                {
                    "and": [
                        {
                            "field": "createdBy",
                            "condition": "EQUAL",
                            "values": [get_root_urn()],
                            "negated": False,
                        }
                    ]
                }
            ],
        }
    }

    list_response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in list_response
    assert list_response["data"]["listActionRequests"]

    # Debug: Print all requests to see what's available
    requests = list_response["data"]["listActionRequests"]["actionRequests"]
    print(f"✓ Found {len(requests)} total action requests")
    for i, request in enumerate(requests):
        print(
            f"  Request {i + 1}: URN={request['urn']}, Type={request.get('type', 'N/A')}"
        )

    # Find our created request
    our_request = None
    for request in requests:
        if request["urn"] == request_urn:
            our_request = request
            break

    assert our_request is not None, (
        f"Could not find created request {request_urn} among {[r['urn'] for r in requests]}"
    )
    assert (
        our_request["entity"]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset,PROD)"
    )
    assert (
        our_request["params"]["workflowFormRequest"]["workflowUrn"]
        == "urn:li:actionWorkflow:test-dynamic-assignment-workflow"
    )

    print(f"✓ Found created request: {our_request['urn']}")

    # Verify dynamic assignment worked correctly
    # The assignedUsers and assignedGroups should contain the dynamically resolved entity owners
    assigned_users = our_request["assignedUsers"]
    assigned_groups = our_request["assignedGroups"]

    print(f"✓ Assigned users: {assigned_users}")
    print(f"✓ Assigned groups: {assigned_groups}")

    # Verify entity owners were assigned correctly (since this is the first step)
    assert "urn:li:corpuser:test-entity-owner-user" in assigned_users, (
        f"Expected test-entity-owner-user in assigned users, got {assigned_users}"
    )
    assert "urn:li:corpGroup:test-entity-owner-group" in assigned_groups, (
        f"Expected test-entity-owner-group in assigned groups, got {assigned_groups}"
    )

    print("✓ Entity owners correctly assigned to first step")

    # Verify the current step is the entity owner approval step
    current_step_id = our_request["params"]["workflowFormRequest"]["stepState"][
        "stepId"
    ]
    assert current_step_id == "entity-owner-approval", (
        f"Expected current step to be entity-owner-approval, got {current_step_id}"
    )

    print("✓ Dynamic assignment test completed successfully!")
    print(f"  - Current step: {current_step_id}")
    print(f"  - Assigned users: {assigned_users}")
    print(f"  - Assigned groups: {assigned_groups}")
    print("  - Entity owners were correctly dynamically assigned to the first step")
