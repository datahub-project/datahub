import pytest

from tests.utils import (
    delete_urns_from_file,
    execute_gql_with_retry,
    ingest_file_via_rest,
)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session):
    print("ingesting action workflow test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    yield
    print("removing action workflow test data")
    delete_urns_from_file(auth_session, "tests/workflows/action_workflows_data.json")


def test_list_all_workflows(auth_session):
    """Test listing all workflows includes our test workflows"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                description
            }
        }
    }
    """

    variables = {
        "input": {
            "start": 0,
            "count": 50,  # Increased to ensure we get all workflows
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]
    assert result["start"] == 0
    assert result["count"] >= 3  # At least the 3 test workflows
    assert result["total"] >= 3
    assert len(result["workflows"]) >= 3

    # Check that our specific test workflows are present
    workflow_names = [w["name"] for w in result["workflows"]]
    assert "Test Access Workflow 1" in workflow_names
    assert "Test Access Workflow 2" in workflow_names
    assert "Test Custom Workflow" in workflow_names


def test_list_workflows_filtered_by_category(auth_session):
    """Test listing workflows filtered by workflow category"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                customCategory
                description
            }
        }
    }
    """

    # Test filtering by ACCESS type
    variables = {"input": {"start": 0, "count": 50, "category": "ACCESS"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]

    # Check that our ACCESS workflows are present
    workflow_names = [w["name"] for w in result["workflows"]]
    assert "Test Access Workflow 1" in workflow_names
    assert "Test Access Workflow 2" in workflow_names

    # All returned workflows should be ACCESS type
    for workflow in result["workflows"]:
        assert workflow["category"] == "ACCESS"


def test_list_workflows_filtered_by_custom_category(auth_session):
    """Test listing workflows filtered by custom category"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                customCategory
                description
            }
        }
    }
    """

    variables = {
        "input": {
            "start": 0,
            "count": 50,
            "category": "CUSTOM",
            "customCategory": "DATA_CLASSIFICATION",
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]

    # Check that our CUSTOM workflow is present
    workflow_names = [w["name"] for w in result["workflows"]]
    assert "Test Custom Workflow" in workflow_names

    # All returned workflows should be CUSTOM type with DATA_CLASSIFICATION
    for workflow in result["workflows"]:
        assert workflow["category"] == "CUSTOM"
        assert workflow["customCategory"] == "DATA_CLASSIFICATION"


def test_list_workflows_filtered_by_entrypoint_type(auth_session):
    """Test listing workflows filtered by entrypoint type"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                trigger {
                    form {
                        entrypoints {
                            type
                            label
                        }
                    }
                }
            }
        }
    }
    """

    # Test filtering by ENTITY_PROFILE entrypoint
    variables = {"input": {"start": 0, "count": 50, "entrypointType": "ENTITY_PROFILE"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]

    # Check that our ENTITY_PROFILE workflows are present
    workflow_names = [w["name"] for w in result["workflows"]]
    assert "Test Access Workflow 1" in workflow_names
    assert "Test Access Workflow 2" in workflow_names

    # All workflows should have at least one ENTITY_PROFILE entrypoint
    for workflow in result["workflows"]:
        if workflow["trigger"]["form"] and workflow["trigger"]["form"]["entrypoints"]:
            entrypoint_types = [
                ep["type"] for ep in workflow["trigger"]["form"]["entrypoints"]
            ]
            assert "ENTITY_PROFILE" in entrypoint_types


def test_list_workflows_filtered_by_entity_type(auth_session):
    """Test listing workflows filtered by entity type"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                trigger {
                    form {
                        entityTypes
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50, "entityType": "DATASET"}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]

    # Check that our dataset workflows are present
    workflow_names = [w["name"] for w in result["workflows"]]
    assert "Test Access Workflow 1" in workflow_names
    assert "Test Access Workflow 2" in workflow_names

    # All workflows should support dataset entity type
    for workflow in result["workflows"]:
        if workflow["trigger"]["form"] and workflow["trigger"]["form"]["entityTypes"]:
            assert "DATASET" in workflow["trigger"]["form"]["entityTypes"]


def test_list_workflows_pagination(auth_session):
    """Test pagination of workflow listing"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
            }
        }
    }
    """

    # Get first page
    variables = {"input": {"start": 0, "count": 2}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]
    assert result["start"] == 0
    assert result["count"] <= 2  # Should return at most 2 workflows
    assert result["total"] >= 3  # Should have at least our 3 test workflows

    # Test pagination - get next page if there are more results
    if result["total"] > 2:
        variables["input"]["start"] = 2
        response = execute_gql_with_retry(auth_session, list_query, variables)

        assert "errors" not in response
        result = response["data"]["listActionWorkflows"]
        assert result["start"] == 2


def test_list_workflows_with_group_resolution(auth_session):
    """Test listing workflows with group information resolved"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                steps {
                    id
                    actors {
                        groups {
                            urn
                            info {
                                displayName
                            }
                        }
                    }
                }
            }
        }
    }
    """

    variables = {"input": {"start": 0, "count": 50}}

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]

    # Find our test workflow with group reviewers
    test_workflow = None
    for workflow in result["workflows"]:
        if workflow["name"] == "Test Access Workflow 1":
            test_workflow = workflow
            break

    assert test_workflow is not None, "Test Access Workflow 1 not found"

    # Check that group information is resolved
    groups = test_workflow["steps"][0]["actors"]["groups"]
    assert len(groups) > 0

    # Check that at least one group has resolved info
    group_with_info = None
    for group in groups:
        if group["info"] and group["info"]["displayName"]:
            group_with_info = group
            break

    assert group_with_info is not None, "No group with resolved info found"


def test_list_workflows_empty_result(auth_session):
    """Test listing workflows with filters that return no results"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
            }
        }
    }
    """

    # Filter by a non-existent custom type
    variables = {
        "input": {
            "start": 0,
            "count": 50,
            "category": "CUSTOM",
            "customCategory": "NON_EXISTENT_TYPE_12345",
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]
    assert result["start"] == 0
    assert result["count"] == 0
    assert result["total"] == 0
    assert len(result["workflows"]) == 0


def test_list_workflows_with_condition_field(auth_session):
    """Test that listActionWorkflows returns the condition field correctly"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                description
                trigger {
                    type
                    form {
                        entityTypes
                        fields {
                            id
                            name
                            description
                            valueType
                            cardinality
                            required
                            condition {
                                type
                                singleFieldValueCondition {
                                    field
                                    values
                                    condition
                                    negated
                                }
                            }
                        }
                        entrypoints {
                            type
                            label
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
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]
    assert len(result["workflows"]) >= 1

    # Check that the trigger structure is correctly returned
    for workflow in result["workflows"]:
        assert "trigger" in workflow
        assert workflow["trigger"]["type"] is not None

        # Check form structure if present
        if workflow["trigger"]["form"] is not None:
            form = workflow["trigger"]["form"]
            assert "fields" in form
            assert "entrypoints" in form

            # Check field structure and condition field
            for field in form["fields"]:
                assert "id" in field
                assert "name" in field
                assert "valueType" in field
                assert "cardinality" in field
                assert "required" in field
                assert "condition" in field

                # If a field has a condition, verify its structure
                if field["condition"] is not None:
                    condition = field["condition"]
                    assert "type" in condition

                    if condition["type"] == "SINGLE_FIELD_VALUE":
                        assert "singleFieldValueCondition" in condition
                        single_condition = condition["singleFieldValueCondition"]
                        assert "field" in single_condition
                        assert "values" in single_condition
                        assert "condition" in single_condition
                        assert "negated" in single_condition

            # Check entrypoints structure
            for entrypoint in form["entrypoints"]:
                assert "type" in entrypoint
                assert "label" in entrypoint

    print(
        "✓ Successfully verified trigger and condition fields are returned in listActionWorkflows"
    )


def test_list_workflows_with_comprehensive_trigger_verification(auth_session):
    """Test that listActionWorkflows returns comprehensive trigger information"""

    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            start
            count
            total
            workflows {
                urn
                name
                category
                customCategory
                description
                trigger {
                    type
                    form {
                        entityTypes
                        fields {
                            id
                            name
                            description
                            valueType
                            allowedEntityTypes
                            allowedValues {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                            cardinality
                            required
                            condition {
                                type
                                singleFieldValueCondition {
                                    field
                                    values
                                    condition
                                    negated
                                }
                            }
                        }
                        entrypoints {
                            type
                            label
                        }
                    }
                }
                steps {
                    id
                    type
                    description
                    actors {
                        users {
                            urn
                            username
                        }
                        groups {
                            urn
                            name
                        }
                        roles {
                            urn
                            name
                        }
                        dynamicAssignment {
                            type
                            ownershipTypes {
                                urn
                                info {
                                    name
                                }
                            }
                        }
                    }
                }
                created {
                    time
                    actor {
                        urn
                        username
                    }
                }
                lastModified {
                    time
                    actor {
                        urn
                        username
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
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, variables)

    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    result = response["data"]["listActionWorkflows"]
    assert len(result["workflows"]) >= 3  # At least our test workflows

    # Verify each workflow has proper trigger structure
    test_workflows_found = []
    for workflow in result["workflows"]:
        # Basic workflow fields
        assert workflow["urn"] is not None
        assert workflow["name"] is not None
        assert workflow["category"] is not None

        # Trigger verification
        assert workflow["trigger"] is not None
        trigger = workflow["trigger"]
        assert trigger["type"] == "FORM_SUBMITTED"  # Currently only type supported

        # Form verification
        if trigger["form"] is not None:
            form = trigger["form"]

            # Entity types can be null or a list
            if form["entityTypes"] is not None:
                assert isinstance(form["entityTypes"], list)

            # Fields should always be present and be a list
            assert form["fields"] is not None
            assert isinstance(form["fields"], list)

            # Entrypoints should always be present and be a list
            assert form["entrypoints"] is not None
            assert isinstance(form["entrypoints"], list)
            assert len(form["entrypoints"]) > 0

            # Verify field structure in detail
            for field in form["fields"]:
                assert field["id"] is not None
                assert field["name"] is not None
                assert field["valueType"] is not None
                assert field["cardinality"] in ["SINGLE", "MULTIPLE"]
                assert isinstance(field["required"], bool)

                # allowedValues can be null or a list
                if field["allowedValues"] is not None:
                    assert isinstance(field["allowedValues"], list)

                # allowedEntityTypes can be null or a list
                if field["allowedEntityTypes"] is not None:
                    assert isinstance(field["allowedEntityTypes"], list)

            # Verify entrypoint structure
            for entrypoint in form["entrypoints"]:
                assert entrypoint["type"] in ["HOME", "ENTITY_PROFILE"]
                assert entrypoint["label"] is not None

        # Steps verification
        if workflow["steps"] is not None:
            for step in workflow["steps"]:
                assert step["id"] is not None
                assert step["type"] is not None

                if step["actors"] is not None:
                    actors = step["actors"]
                    # Users, groups, and roles should be lists (can be empty)
                    assert isinstance(actors["users"], list)
                    assert isinstance(actors["groups"], list)
                    assert isinstance(actors["roles"], list)

        # Track our test workflows
        if workflow["name"] in [
            "Test Access Workflow 1",
            "Test Access Workflow 2",
            "Test Custom Workflow",
        ]:
            test_workflows_found.append(workflow["name"])

    # Ensure we found our test workflows
    assert "Test Access Workflow 1" in test_workflows_found
    assert "Test Access Workflow 2" in test_workflows_found
    assert "Test Custom Workflow" in test_workflows_found

    print(
        f"✓ Successfully verified {len(result['workflows'])} workflows with comprehensive trigger structure"
    )
    print(f"✓ Found test workflows: {test_workflows_found}")
