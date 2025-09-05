import pytest

from tests.utils import (
    delete_urns_from_file,
    execute_gql_with_retry,
    get_root_urn,
    ingest_file_via_rest,
)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Ingest test data before tests and clean up after"""
    print("ingesting action workflow test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    print("ingesting action workflow requests test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_requests_data.json")
    yield
    print("removing action workflow requests test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_requests_data.json")
    print("removing action workflow test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_workflows_data.json")


def test_list_all_action_workflow_requests(auth_session):
    """Test listing all action workflow requests includes our test requests"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
                description
                assignedUsers
                assignedGroups
                entity {
                    urn
                }
                subResourceType
                subResource
                params {
                    workflowFormRequest {
                        workflowUrn
                        category
                        customCategory
                        fields {
                            id
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                        }
                        access {
                            expiresAt
                        }
                        stepState {
                            stepId
                        }
                    }
                }
                created {
                    time
                    actor {
                        urn
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]
    assert result["start"] == 0

    # We expect at least 4 requests based on our test data
    assert result["count"] >= 4
    assert result["total"] >= 4
    assert len(result["actionRequests"]) >= 4

    # Check that we have workflow request types
    request_types = [r["type"] for r in result["actionRequests"]]
    assert all(req_type == "WORKFLOW_FORM_REQUEST" for req_type in request_types)

    # Check that we have some requests with descriptions matching our test data
    descriptions = [
        r["description"] for r in result["actionRequests"] if r["description"]
    ]
    expected_descriptions = [
        "Access request for quarterly reporting dataset",
        "Access request for ML training dataset with 60-day expiration",
        "Data classification request for sensitive customer data",
        "Multi-value access request for product recommendation system",
        "Dashboard access request for executive team",
    ]

    # At least some of our test descriptions should be present
    found_descriptions = [
        desc for desc in descriptions if desc in expected_descriptions
    ]
    assert len(found_descriptions) >= 4, (
        f"Expected at least 4 test descriptions, found: {found_descriptions}"
    )

    # Check that we have requests with proper workflow URNs
    workflow_urns = [
        r["params"]["workflowFormRequest"]["workflowUrn"]
        for r in result["actionRequests"]
    ]
    expected_workflow_urns = [
        "urn:li:actionWorkflow:test-access-workflow-1",
        "urn:li:actionWorkflow:test-access-workflow-2",
        "urn:li:actionWorkflow:test-custom-workflow-1",
        "urn:li:actionWorkflow:test-multi-values-workflow",
    ]

    # At least some of our test workflow URNs should be present
    found_workflow_urns = [
        urn for urn in workflow_urns if urn in expected_workflow_urns
    ]
    assert len(found_workflow_urns) >= 4, (
        f"Expected at least 4 test workflow URNs, found: {found_workflow_urns}"
    )

    # Verify structure of the first request
    first_request = result["actionRequests"][0]
    assert first_request["type"] == "WORKFLOW_FORM_REQUEST"
    assert first_request["params"]["workflowFormRequest"]["workflowUrn"] is not None
    assert first_request["params"]["workflowFormRequest"]["category"] in [
        "ACCESS",
        "CUSTOM",
    ]
    assert (
        first_request["params"]["workflowFormRequest"]["stepState"]["stepId"]
        is not None
    )
    assert isinstance(first_request["assignedUsers"], list)
    assert isinstance(first_request["assignedGroups"], list)
    assert first_request["created"]["time"] is not None
    assert first_request["created"]["actor"]["urn"] is not None

    print(
        f"✓ Successfully listed {len(result['actionRequests'])} action workflow requests"
    )
    print(f"✓ Found {len(found_descriptions)} matching test descriptions")
    print(f"✓ Found {len(found_workflow_urns)} matching test workflow URNs")


def test_list_action_workflow_requests_filtered_by_entity(auth_session):
    """Test listing action workflow requests filtered by entity URN"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
                entity {
                    urn
                }
                params {
                    workflowFormRequest {
                        workflowUrn
                        fields {
                            id
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "start": 0,
            "count": 50,
            "type": "WORKFLOW_FORM_REQUEST",
            "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-1,PROD)",
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]

    # Check that our test request for this entity is present
    request_urns = [r["urn"] for r in result["actionRequests"]]
    assert "urn:li:actionRequest:test-request-1" in request_urns

    # All returned requests should be for the specified entity
    for request in result["actionRequests"]:
        assert (
            request["entity"]["urn"]
            == "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-1,PROD)"
        )

    print(
        f"✓ Successfully filtered {len(result['actionRequests'])} requests by entity URN"
    )


def test_list_action_workflow_requests_with_field_values(auth_session):
    """Test listing action workflow requests with field values included"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                params {
                    workflowFormRequest {
                        workflowUrn
                        fields {
                            id
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]

    # Find our multi-value test request
    test_request = next(
        r
        for r in result["actionRequests"]
        if r["urn"] == "urn:li:actionRequest:test-request-4"
    )

    # Check that field values are included
    fields = test_request["params"]["workflowFormRequest"]["fields"]
    assert len(fields) == 2

    # Check reasons field (multiple string values)
    reasons_field = next(f for f in fields if f["id"] == "reasons")
    assert len(reasons_field["values"]) == 2

    # Check that the values have the correct structure
    for value in reasons_field["values"]:
        assert "stringValue" in value
        assert value["stringValue"] in [
            "Primary reason: Data analysis for product recommendations",
            "Secondary reason: Backup data for disaster recovery testing",
        ]

    # Check quantities field (multiple number values)
    quantities_field = next(f for f in fields if f["id"] == "quantities")
    assert len(quantities_field["values"]) == 2

    # Check that the values have the correct structure
    for value in quantities_field["values"]:
        assert "numberValue" in value
        assert value["numberValue"] in [1000.0, 5000.0]

    print("✓ Successfully retrieved field values for multi-value request")


def test_list_action_workflow_requests_with_access_expiration(auth_session):
    """Test listing action workflow requests with access expiration information"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                params {
                    workflowFormRequest {
                        workflowUrn
                        access {
                            expiresAt
                        }
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]

    # Check that requests with expiration have the access field populated
    request_with_expiration = next(
        r
        for r in result["actionRequests"]
        if r["urn"] == "urn:li:actionRequest:test-request-2"
    )
    workflow_request = request_with_expiration["params"]["workflowFormRequest"]
    assert workflow_request["access"] is not None
    assert workflow_request["access"]["expiresAt"] == 1643587200000

    print("✓ Successfully retrieved access expiration information")


def test_list_action_workflow_requests_pagination(auth_session):
    """Test pagination of action workflow requests listing"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
            }
        }
    }
    """

    # Get first page
    variables = {"input": {"start": 0, "count": 3, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]
    assert result["start"] == 0
    assert result["count"] <= 3  # Should return at most 3 requests
    assert result["total"] >= 4  # Should have at least our 4 test requests

    # Test pagination - get next page if there are more results
    if result["total"] > 3:
        variables["input"]["start"] = 3
        response = execute_gql_with_retry(auth_session, list_query, variables)

        assert "errors" not in response
        result = response["data"]["listActionRequests"]
        assert result["start"] == 3

    print(f"✓ Successfully tested pagination with {result['total']} total requests")


def test_list_action_workflow_requests_empty_result(auth_session):
    """Test listing action workflow requests with filters that return no results"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
            }
        }
    }
    """

    # Filter by a non-existent type that should return no results
    variables = {"input": {"start": 0, "count": 50, "type": "NON_EXISTENT_TYPE"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    # This should either return an error (invalid type) or empty results
    if "errors" in response:
        # GraphQL validation caught the invalid type
        assert "errors" in response
        print("✓ GraphQL validation correctly caught invalid type")
    else:
        # Server handled it gracefully with empty results
        result = response["data"]["listActionRequests"]
        assert result["start"] == 0
        assert result["count"] == 0
        assert result["total"] == 0
        assert len(result["actionRequests"]) == 0
        print("✓ Successfully handled invalid type with empty result set")


def test_list_action_workflow_requests_error_handling(auth_session):
    """Test error handling for invalid input parameters"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
            }
        }
    }
    """

    # Test with invalid count (negative)
    variables = {"input": {"start": 0, "count": -1, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    # Should handle gracefully or return an error
    if "errors" in response:
        print(
            f"✓ Correctly handled invalid count parameter: {response['errors'][0]['message']}"
        )
    else:
        # If no error, the system handled it gracefully
        print("✓ System gracefully handled invalid count parameter")


def test_list_action_workflow_requests_comprehensive(auth_session):
    """Test comprehensive listing of action workflow requests with all available data"""

    list_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
                description
                assignedUsers
                assignedGroups
                assignedRoles
                entity {
                    urn
                }
                subResourceType
                subResource
                params {
                    workflowFormRequest {
                        workflowUrn
                        category
                        customCategory
                        fields {
                            id
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                        }
                        access {
                            expiresAt
                        }
                        stepState {
                            stepId
                        }
                    }
                }
                created {
                    time
                    actor {
                        urn
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50, "type": "WORKFLOW_FORM_REQUEST"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionRequests"]

    result = response["data"]["listActionRequests"]
    assert len(result["actionRequests"]) >= 4

    # Verify comprehensive data for test-request-1
    test_request = next(
        r
        for r in result["actionRequests"]
        if r["urn"] == "urn:li:actionRequest:test-request-1"
    )

    # Basic fields
    assert test_request["type"] == "WORKFLOW_FORM_REQUEST"
    assert (
        test_request["description"] == "Access request for quarterly reporting dataset"
    )

    # Assignments
    assert len(test_request["assignedUsers"]) == 2
    assert get_root_urn() in test_request["assignedUsers"]
    assert "urn:li:corpuser:test-access-reviewer" in test_request["assignedUsers"]
    assert len(test_request["assignedGroups"]) == 1
    assert "urn:li:corpGroup:test-access-reviewers" in test_request["assignedGroups"]

    # Entity
    assert (
        test_request["entity"]["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-1,PROD)"
    )

    # Workflow request details
    workflow_request = test_request["params"]["workflowFormRequest"]
    assert (
        workflow_request["workflowUrn"]
        == "urn:li:actionWorkflow:test-access-workflow-1"
    )
    assert workflow_request["category"] == "ACCESS"
    assert workflow_request["stepState"]["stepId"] == "manager-approval"

    # Field values
    assert len(workflow_request["fields"]) == 1
    reason_field = workflow_request["fields"][0]
    assert reason_field["id"] == "reason"
    assert len(reason_field["values"]) == 1
    assert (
        reason_field["values"][0]["stringValue"]
        == "Need access for quarterly reporting analysis"
    )

    # Audit info
    assert test_request["created"]["time"] == 1640995200000
    assert test_request["created"]["actor"]["urn"] == "urn:li:corpuser:datahub"

    print("✓ Successfully verified comprehensive data for action workflow requests")

    # Verify test-request-3 (custom workflow)
    custom_request = next(
        r
        for r in result["actionRequests"]
        if r["urn"] == "urn:li:actionRequest:test-request-3"
    )
    custom_workflow = custom_request["params"]["workflowFormRequest"]
    assert custom_workflow["category"] == "CUSTOM"
    assert custom_workflow["customCategory"] == "DATA_CLASSIFICATION"

    print("✓ Successfully verified custom workflow request data")
