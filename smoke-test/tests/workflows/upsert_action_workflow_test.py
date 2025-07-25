import time

import pytest

from tests.utils import (
    delete_urns_from_file,
    execute_gql,
    get_root_urn,
    ingest_file_via_rest,
)

# Test workflows that will be created/updated during tests
CREATED_WORKFLOW_URNS: list[str] = []


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Ingest test data before tests and clean up after"""
    print("ingesting action workflow test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    yield
    print("removing action workflow test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_workflows_data.json")
    # Clean up any workflows created during testing
    for urn in CREATED_WORKFLOW_URNS:
        try:
            graph_client.hard_delete_entity(urn)
        except Exception:
            pass  # Ignore errors during cleanup


def test_upsert_create_new_workflow(auth_session):
    """Test creating a new workflow via upsert mutation"""

    # Create a new workflow
    workflow_name = f"Test Workflow {int(time.time())}"

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            description
            trigger {
                form {
                    entityTypes
                    fields {
                        id
                        name
                        description
                        valueType
                        cardinality
                        required
                    }
                    entrypoints {
                        type
                        label
                    }
                }
            }
            steps {
                id
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
    """

    variables = {
        "input": {
            "name": workflow_name,
            "category": "ACCESS",
            "description": "Test workflow created via API",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        }
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "approval-step",
                    "description": "Approval step",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": [],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"] is not None

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == workflow_name
    assert workflow["category"] == "ACCESS"
    assert workflow["description"] == "Test workflow created via API"
    assert len(workflow["trigger"]["form"]["entityTypes"]) == 1
    assert "DATASET" in workflow["trigger"]["form"]["entityTypes"]
    assert len(workflow["trigger"]["form"]["fields"]) == 1
    assert workflow["trigger"]["form"]["fields"][0]["id"] == "reason"
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 1
    assert workflow["trigger"]["form"]["entrypoints"][0]["type"] == "ENTITY_PROFILE"
    assert len(workflow["steps"]) == 1
    assert workflow["steps"][0]["id"] == "approval-step"
    assert len(workflow["steps"][0]["actors"]["users"]) == 1
    assert workflow["steps"][0]["actors"]["users"][0]["urn"] == get_root_urn()
    assert workflow["created"]["time"] is not None
    assert workflow["created"]["actor"]["urn"] == get_root_urn()
    assert workflow["lastModified"]["time"] is not None
    assert workflow["lastModified"]["actor"]["urn"] == get_root_urn()

    # Track the created workflow for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])

    print(f"✓ Successfully created workflow: {workflow['urn']}")


def test_upsert_update_existing_workflow(auth_session):
    """Test updating an existing workflow via upsert mutation"""

    existing_workflow_urn = "urn:li:actionWorkflow:test-access-workflow-1"
    updated_name = f"Updated Test Workflow {int(time.time())}"

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            description
            trigger {
                form {
                    entityTypes
                    fields {
                        id
                        name
                        description
                        valueType
                        cardinality
                        required
                    }
                    entrypoints {
                        type
                        label
                    }
                }
            }
            steps {
                id
                description
                actors {
                    users {
                        urn
                    }
                    groups {
                        urn
                    }
                    roles {
                        urn
                    }
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
    """

    variables = {
        "input": {
            "urn": existing_workflow_urn,
            "name": updated_name,
            "category": "ACCESS",
            "description": "Updated test workflow description",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET", "DASHBOARD"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "duration",
                            "name": "Access Duration",
                            "description": "How long do you need access?",
                            "valueType": "NUMBER",
                            "cardinality": "SINGLE",
                            "required": False,
                        },
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Data Access"},
                        {"type": "HOME", "label": "Request Access"},
                    ],
                },
            },
            "steps": [
                {
                    "id": "manager-approval",
                    "description": "Manager approval step",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": [],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"]

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["urn"] == existing_workflow_urn
    assert workflow["name"] == updated_name
    assert workflow["category"] == "ACCESS"
    assert workflow["description"] == "Updated test workflow description"
    assert set(workflow["trigger"]["form"]["entityTypes"]) == {"DATASET", "DASHBOARD"}
    assert len(workflow["trigger"]["form"]["fields"]) == 2
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 2
    assert len(workflow["steps"]) == 1
    assert workflow["lastModified"]["actor"]["urn"] == get_root_urn()


def test_upsert_custom_workflow(auth_session):
    """Test creating a custom workflow with custom type"""

    workflow_name = f"Custom Workflow {int(time.time())}"

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            customCategory
            description
            trigger {
                form {
                    fields {
                        id
                        name
                        valueType
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
                    }
                    entrypoints {
                        type
                        label
                    }
                }
            }
            steps {
                id
                description
                actors {
                    users {
                        urn
                    }
                    groups {
                        urn
                    }
                    roles {
                        urn
                    }
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "name": workflow_name,
            "category": "CUSTOM",
            "customCategory": "DATA_CLASSIFICATION",
            "description": "Custom workflow for data classification",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "fields": [
                        {
                            "id": "classification",
                            "name": "Classification Level",
                            "description": "Select classification level",
                            "valueType": "STRING",
                            "allowedValues": [
                                {"stringValue": "PUBLIC"},
                                {"stringValue": "INTERNAL"},
                                {"stringValue": "CONFIDENTIAL"},
                            ],
                            "cardinality": "SINGLE",
                            "required": True,
                        }
                    ],
                    "entrypoints": [
                        {"type": "HOME", "label": "Request Classification"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "security-review",
                    "description": "Security team review",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": ["urn:li:corpGroup:security-team"],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"]

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == workflow_name
    assert workflow["category"] == "CUSTOM"
    assert workflow["customCategory"] == "DATA_CLASSIFICATION"
    assert workflow["description"] == "Custom workflow for data classification"
    assert len(workflow["trigger"]["form"]["fields"]) == 1
    assert workflow["trigger"]["form"]["fields"][0]["id"] == "classification"
    assert workflow["trigger"]["form"]["fields"][0]["name"] == "Classification Level"
    assert workflow["trigger"]["form"]["fields"][0]["valueType"] == "STRING"
    assert len(workflow["trigger"]["form"]["fields"][0]["allowedValues"]) == 3
    assert workflow["trigger"]["form"]["fields"][0]["cardinality"] == "SINGLE"
    assert workflow["trigger"]["form"]["fields"][0]["required"]
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 1
    assert workflow["trigger"]["form"]["entrypoints"][0]["type"] == "HOME"
    assert (
        workflow["trigger"]["form"]["entrypoints"][0]["label"]
        == "Request Classification"
    )
    assert len(workflow["steps"]) == 1
    assert workflow["steps"][0]["id"] == "security-review"
    assert workflow["steps"][0]["description"] == "Security team review"
    assert (
        workflow["steps"][0]["actors"]["groups"][0]["urn"]
        == "urn:li:corpGroup:security-team"
    )

    # Track for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])


def test_upsert_workflow_with_complex_reviewers(auth_session):
    """Test creating a workflow with users, groups, and dynamic assignment reviewers all together"""

    workflow_name = f"Complex Reviewer Workflow {int(time.time())}"

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            description
            trigger {
                form {
                    entityTypes
                    fields {
                        id
                        name
                        description
                        valueType
                        cardinality
                        required
                    }
                    entrypoints {
                        type
                        label
                    }
                }
            }
            steps {
                id
                description
                actors {
                    users {
                        urn
                        username
                        info {
                            active
                            displayName
                            email
                            title
                            fullName
                            firstName
                            lastName
                            departmentName
                        }
                    }
                    groups {
                        urn
                        name
                        info {
                            displayName
                            description
                            email
                        }
                    }
                    roles {
                        urn
                    }
                    dynamicAssignment {
                        type
                        ownershipTypes {
                            urn
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
    """

    variables = {
        "input": {
            "name": workflow_name,
            "category": "ACCESS",
            "description": "Test workflow with users, groups, and dynamic assignment reviewers",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "duration",
                            "name": "Access Duration",
                            "description": "How long do you need access?",
                            "valueType": "NUMBER",
                            "cardinality": "SINGLE",
                            "required": False,
                        },
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "comprehensive-approval",
                    "description": "Comprehensive approval with all reviewer types",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [
                            get_root_urn(),
                            "urn:li:corpuser:test-access-reviewer",
                        ],
                        "groupUrns": ["urn:li:corpGroup:test-access-reviewers"],
                        "roleUrns": [],
                        "dynamicAssignment": {
                            "type": "ENTITY_OWNERS",
                            "ownershipTypeUrns": [],
                        },
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"] is not None

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == workflow_name
    assert workflow["category"] == "ACCESS"
    assert (
        workflow["description"]
        == "Test workflow with users, groups, and dynamic assignment reviewers"
    )
    assert len(workflow["trigger"]["form"]["entityTypes"]) == 1
    assert "DATASET" in workflow["trigger"]["form"]["entityTypes"]
    assert len(workflow["trigger"]["form"]["fields"]) == 2
    assert workflow["trigger"]["form"]["fields"][0]["id"] == "reason"
    assert workflow["trigger"]["form"]["fields"][1]["id"] == "duration"
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 1
    assert workflow["trigger"]["form"]["entrypoints"][0]["type"] == "ENTITY_PROFILE"
    assert len(workflow["steps"]) == 1
    assert workflow["steps"][0]["id"] == "comprehensive-approval"

    # Test user actors - should have both admin and test-access-reviewer
    assert len(workflow["steps"][0]["actors"]["users"]) == 2
    user_actors = workflow["steps"][0]["actors"]["users"]

    # Find admin user
    admin_user = next(u for u in user_actors if u["urn"] == get_root_urn())
    assert admin_user["urn"] == get_root_urn()
    assert admin_user["username"] == "admin"

    # Find test user - key test for user reviewer functionality
    test_user = next(
        u for u in user_actors if u["urn"] == "urn:li:corpuser:test-access-reviewer"
    )
    assert test_user["urn"] == "urn:li:corpuser:test-access-reviewer"
    assert test_user["username"] == "test-access-reviewer"
    assert test_user["info"]["active"]
    assert test_user["info"]["displayName"] == "Test Access Reviewer"
    assert test_user["info"]["email"] == "test-access-reviewer@company.com"
    assert test_user["info"]["title"] == "Data Access Reviewer"
    assert test_user["info"]["fullName"] == "Test Access Reviewer"
    assert test_user["info"]["firstName"] == "Test"
    assert test_user["info"]["lastName"] == "Reviewer"
    assert test_user["info"]["departmentName"] == "Data Governance"

    # Test group actor - key test for group reviewer functionality
    assert len(workflow["steps"][0]["actors"]["groups"]) == 1
    group_actor = workflow["steps"][0]["actors"]["groups"][0]
    assert group_actor["urn"] == "urn:li:corpGroup:test-access-reviewers"
    assert group_actor["name"] == "test-access-reviewers"
    assert group_actor["info"]["displayName"] == "Test Access Reviewers Group"
    assert (
        group_actor["info"]["description"]
        == "Group for reviewing access requests in test workflows"
    )
    assert group_actor["info"]["email"] == "test-access-reviewers@company.com"

    # Test dynamic assignment - key test for dynamic assignment functionality
    dynamic_assignment = workflow["steps"][0]["actors"]["dynamicAssignment"]
    assert dynamic_assignment is not None
    assert dynamic_assignment["type"] == "ENTITY_OWNERS"
    assert dynamic_assignment["ownershipTypes"] == []  # Empty array for general owners

    assert workflow["created"]["time"] is not None
    assert workflow["created"]["actor"]["urn"] == get_root_urn()
    assert workflow["lastModified"]["time"] is not None
    assert workflow["lastModified"]["actor"]["urn"] == get_root_urn()

    # Track the created workflow for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])

    print(f"✓ Successfully created workflow with complex reviewers: {workflow['urn']}")
    print(f"✓ User reviewers: {len(user_actors)} users")
    print(f"✓ Test user reviewer details resolved: {test_user['info']['displayName']}")
    print(f"✓ Group reviewers: {len(workflow['steps'][0]['actors']['groups'])} groups")
    print(f"✓ Group reviewer details resolved: {group_actor['info']['displayName']}")
    print(f"✓ Dynamic assignment: {dynamic_assignment['type']}")
    print(
        "✓ All three reviewer types (users, groups, dynamic assignment) working together!"
    )


def test_upsert_workflow_invalid_user_urn(auth_session):
    """Test error handling for invalid user URN format"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
        }
    }
    """

    variables = {
        "input": {
            "name": "Test Invalid User URN",
            "category": "ACCESS",
            "description": "Test workflow with invalid user URN",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        }
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "approval-step",
                    "description": "Approval step",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": ["invalid-user-urn-format"]  # Invalid URN format
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    # Should return an error due to invalid URN format
    assert "errors" in response
    assert len(response["errors"]) > 0


def test_upsert_workflow_invalid_group_urn(auth_session):
    """Test error handling for invalid group URN format"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
        }
    }
    """

    variables = {
        "input": {
            "name": "Test Invalid Group URN",
            "category": "ACCESS",
            "description": "Test workflow with invalid group URN",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        }
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "approval-step",
                    "description": "Approval step",
                    "type": "APPROVAL",
                    "actors": {
                        "groupUrns": ["not-a-valid-group-urn"]  # Invalid URN format
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    # Should return an error due to invalid URN format
    assert "errors" in response
    assert len(response["errors"]) > 0


def test_upsert_workflow_invalid_workflow_urn(auth_session):
    """Test error handling for invalid workflow URN format when updating"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
        }
    }
    """

    variables = {
        "input": {
            "urn": "invalid-workflow-urn",  # Invalid URN format
            "name": "Test Invalid Workflow URN",
            "category": "ACCESS",
            "description": "Test workflow with invalid workflow URN",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [
                        {
                            "id": "reason",
                            "name": "Access Reason",
                            "description": "Why do you need access?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        }
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "approval-step",
                    "type": "APPROVAL",
                    "description": "Approval step",
                    "actors": {"userUrns": [get_root_urn()]},
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    # Should return an error due to invalid URN format
    assert "errors" in response
    assert len(response["errors"]) > 0


def test_upsert_workflow_missing_required_fields(auth_session):
    """Test error handling for missing required fields"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
        }
    }
    """

    # Missing required fields like name, category, trigger, steps
    variables = {"input": {"description": "Test workflow with missing required fields"}}

    response = execute_gql(auth_session, upsert_query, variables)

    # Should return an error due to missing required fields
    assert "errors" in response
    assert len(response["errors"]) > 0


def test_upsert_workflow_empty_fields_array(auth_session):
    """Test that empty arrays are acceptable for optional fields"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            description
            trigger {
                form {
                    fields {
                        id
                        name
                    }
                    entrypoints {
                        type
                        label
                    }
                }
            }
            steps {
                id
                description
            }
        }
    }
    """

    variables = {
        "input": {
            "name": "Test Empty Arrays",
            "category": "ACCESS",
            "description": "Test workflow with empty optional arrays",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entityTypes": ["DATASET"],
                    "fields": [],  # Empty fields array - this is valid
                    "entrypoints": [],  # Empty entrypoints array - this is valid
                },
            },
            "steps": [],  # Empty steps array - this is valid
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    # Should succeed - empty arrays are valid
    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"] is not None

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == "Test Empty Arrays"
    assert workflow["category"] == "ACCESS"
    assert workflow["description"] == "Test workflow with empty optional arrays"
    assert len(workflow["trigger"]["form"]["fields"]) == 0
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 0
    assert len(workflow["steps"]) == 0

    # Track for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])

    print(f"✓ Successfully created workflow with empty arrays: {workflow['urn']}")


def test_upsert_workflow_with_complex_fields(auth_session):
    """Test creating workflow with all field types and cardinalities"""

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            trigger {
                form {
                    fields {
                        id
                        name
                        description
                        valueType
                        cardinality
                        required
                        allowedValues {
                            ... on StringValue {
                                stringValue
                            }
                            ... on NumberValue {
                                numberValue
                            }
                        }
                        allowedEntityTypes
                    }
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "name": "Complex Fields Workflow",
            "category": "CUSTOM",
            "customCategory": "COMPLEX_WORKFLOW",
            "description": "Workflow testing all field types and cardinalities",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "fields": [
                        {
                            "id": "single_string",
                            "name": "Single String Field",
                            "description": "A simple string field",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "priority_level",
                            "name": "Priority Level",
                            "description": "Priority with allowed numeric values",
                            "valueType": "NUMBER",
                            "allowedValues": [
                                {"numberValue": 1},
                                {"numberValue": 2},
                                {"numberValue": 3},
                                {"numberValue": 4},
                                {"numberValue": 5},
                            ],
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "related_entities",
                            "name": "Related Entities",
                            "description": "Multiple URNs pointing to datasets or dashboards",
                            "valueType": "URN",
                            "allowedEntityTypes": ["DATASET", "DASHBOARD"],
                            "cardinality": "MULTIPLE",
                            "required": False,
                        },
                        {
                            "id": "detailed_explanation",
                            "name": "Detailed Explanation",
                            "description": "Rich text explanation with formatting",
                            "valueType": "RICH_TEXT",
                            "cardinality": "SINGLE",
                            "required": False,
                        },
                        {
                            "id": "categories",
                            "name": "Categories",
                            "description": "Multiple string categories from predefined list",
                            "valueType": "STRING",
                            "allowedValues": [
                                {"stringValue": "ANALYTICS"},
                                {"stringValue": "REPORTING"},
                                {"stringValue": "MACHINE_LEARNING"},
                                {"stringValue": "DATA_SCIENCE"},
                            ],
                            "cardinality": "MULTIPLE",
                            "required": False,
                        },
                    ],
                    "entrypoints": [{"type": "HOME", "label": "Complex Request"}],
                },
            },
            "steps": [
                {
                    "id": "review",
                    "description": "Review complex request",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": [],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"]

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == "Complex Fields Workflow"
    assert len(workflow["trigger"]["form"]["fields"]) == 5

    # Create a mapping for easier field verification
    fields_by_id = {
        field["id"]: field for field in workflow["trigger"]["form"]["fields"]
    }

    # Verify STRING field (single)
    string_field = fields_by_id["single_string"]
    assert string_field["name"] == "Single String Field"
    assert string_field["valueType"] == "STRING"
    assert string_field["cardinality"] == "SINGLE"
    assert string_field["required"]
    assert string_field["description"] == "A simple string field"

    # Verify NUMBER field with allowed values (single)
    number_field = fields_by_id["priority_level"]
    assert number_field["name"] == "Priority Level"
    assert number_field["valueType"] == "NUMBER"
    assert number_field["cardinality"] == "SINGLE"
    assert number_field["required"]
    assert len(number_field["allowedValues"]) == 5
    allowed_numbers = [v["numberValue"] for v in number_field["allowedValues"]]
    assert set(allowed_numbers) == {1, 2, 3, 4, 5}

    # Verify URN field with allowed entity types (multiple)
    urn_field = fields_by_id["related_entities"]
    assert urn_field["name"] == "Related Entities"
    assert urn_field["valueType"] == "URN"
    assert urn_field["cardinality"] == "MULTIPLE"
    assert not urn_field["required"]
    assert set(urn_field["allowedEntityTypes"]) == {"DATASET", "DASHBOARD"}

    # Verify RICH_TEXT field (single, freeform)
    rich_text_field = fields_by_id["detailed_explanation"]
    assert rich_text_field["name"] == "Detailed Explanation"
    assert rich_text_field["valueType"] == "RICH_TEXT"
    assert rich_text_field["cardinality"] == "SINGLE"
    assert not rich_text_field["required"]
    assert rich_text_field["description"] == "Rich text explanation with formatting"
    # Rich text should not have allowedValues (freeform)
    assert not rich_text_field["allowedValues"]

    # Verify STRING field with allowed values (multiple)
    categories_field = fields_by_id["categories"]
    assert categories_field["name"] == "Categories"
    assert categories_field["valueType"] == "STRING"
    assert categories_field["cardinality"] == "MULTIPLE"
    assert not categories_field["required"]
    assert len(categories_field["allowedValues"]) == 4
    allowed_categories = [v["stringValue"] for v in categories_field["allowedValues"]]
    assert set(allowed_categories) == {
        "ANALYTICS",
        "REPORTING",
        "MACHINE_LEARNING",
        "DATA_SCIENCE",
    }


# Error handling tests
def test_upsert_workflow_invalid_urn(auth_session):
    """Test that invalid URN format returns an error"""

    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
        }
    }
    """,
        {
            "input": {
                "urn": "invalid-urn-format",
                "name": "Test",
                "category": "ACCESS",
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [{"type": "HOME", "label": "Test"}],
                    },
                },
                "steps": [
                    {
                        "id": "step1",
                        "type": "APPROVAL",
                        "actors": {"userUrns": [], "groupUrns": [], "roleUrns": []},
                    }
                ],
            }
        },
    )

    assert "errors" in response
    # Server returns generic error message for invalid URN format


def test_upsert_workflow_missing_required_field(auth_session):
    """Test that missing required fields are caught by GraphQL schema validation"""

    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
        }
    }
    """,
        {
            "input": {
                # Missing "name" field which is required by GraphQL schema
                "category": "ACCESS",
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [],
                    },
                },
                "steps": [],
            }
        },
    )

    assert "errors" in response
    # GraphQL schema validation should catch missing required fields like "name"


def test_upsert_workflow_empty_steps_array(auth_session):
    """Test that empty steps array is accepted (server allows it)"""

    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            steps {
                id
            }
        }
    }
    """,
        {
            "input": {
                "name": "No Steps Workflow",
                "category": "ACCESS",
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [{"type": "HOME", "label": "Test"}],
                    },
                },
                "steps": [],
            }
        },
    )

    # Server allows empty steps array
    assert "errors" not in response
    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == "No Steps Workflow"
    assert len(workflow["steps"]) == 0

    # Track for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])


def test_upsert_workflow_invalid_enum_values(auth_session):
    """Test that invalid enum values are caught by GraphQL schema validation"""

    # This test verifies that GraphQL schema validation catches invalid enum values
    # before they reach the server resolver
    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
        }
    }
    """,
        {
            "input": {
                "name": "Invalid Enum Workflow",
                "category": "INVALID_TYPE",  # This will be caught by GraphQL schema validation
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [{"type": "HOME", "label": "Test"}],
                    },
                },
                "steps": [],
            }
        },
    )

    assert "errors" in response
    # GraphQL schema validation should catch invalid enum values


def test_upsert_workflow_empty_entrypoints_array(auth_session):
    """Test that empty entrypoints array is accepted (server allows it)"""

    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            trigger {
                form {
                    entrypoints {
                        type
                    }
                }
            }
        }
    }
    """,
        {
            "input": {
                "name": "No Entrypoints Workflow",
                "category": "ACCESS",
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [],
                    },
                },
                "steps": [],
            }
        },
    )

    # Server allows empty entrypoints array
    assert "errors" not in response
    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == "No Entrypoints Workflow"
    assert len(workflow["trigger"]["form"]["entrypoints"]) == 0

    # Track for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])


def test_upsert_workflow_empty_actors(auth_session):
    """Test that step with empty actors is accepted (server allows it)"""

    response = execute_gql(
        auth_session,
        """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            steps {
                id
                actors {
                    users {
                        urn
                    }
                    groups {
                        urn
                    }
                }
            }
        }
    }
    """,
        {
            "input": {
                "name": "Empty Actors Workflow",
                "category": "ACCESS",
                "trigger": {
                    "type": "FORM_SUBMITTED",
                    "form": {
                        "fields": [],
                        "entrypoints": [{"type": "HOME", "label": "Test"}],
                    },
                },
                "steps": [
                    {
                        "id": "step1",
                        "type": "APPROVAL",
                        "actors": {"userUrns": [], "groupUrns": [], "roleUrns": []},
                    }
                ],
            }
        },
    )

    # Server allows empty actors (empty arrays)
    assert "errors" not in response
    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == "Empty Actors Workflow"
    assert len(workflow["steps"]) == 1
    assert workflow["steps"][0]["id"] == "step1"
    assert len(workflow["steps"][0]["actors"]["users"]) == 0
    assert len(workflow["steps"][0]["actors"]["groups"]) == 0

    # Track for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])


def test_upsert_workflow_with_condition_field(auth_session):
    """Test creating a workflow with a condition field"""

    # Create a new workflow with condition field
    workflow_name = f"Test Workflow With Condition {int(time.time())}"

    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
            description
            trigger {
                form {
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
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "name": workflow_name,
            "category": "CUSTOM",
            "description": "Test workflow with condition field",
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "fields": [
                        {
                            "id": "approval_type",
                            "name": "Approval Type",
                            "description": "Type of approval needed",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "detailed_reason",
                            "name": "Detailed Reason",
                            "description": "Detailed reason for request",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": False,
                            "condition": {
                                "type": "SINGLE_FIELD_VALUE",
                                "singleFieldValueCondition": {
                                    "field": "approval_type",
                                    "values": ["ADVANCED"],
                                    "condition": "EQUAL",
                                    "negated": False,
                                },
                            },
                        },
                    ],
                    "entrypoints": [
                        {"type": "ENTITY_PROFILE", "label": "Request Access"}
                    ],
                },
            },
            "steps": [
                {
                    "id": "approval-step",
                    "description": "Approval step",
                    "type": "APPROVAL",
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": [],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql(auth_session, upsert_query, variables)

    assert "errors" not in response
    assert response["data"]["upsertActionWorkflow"] is not None

    workflow = response["data"]["upsertActionWorkflow"]
    assert workflow["name"] == workflow_name
    assert workflow["category"] == "CUSTOM"
    assert len(workflow["trigger"]["form"]["fields"]) == 2

    # Verify first field has no condition
    first_field = workflow["trigger"]["form"]["fields"][0]
    assert first_field["id"] == "approval_type"
    assert first_field["condition"] is None

    # Verify second field has condition
    second_field = workflow["trigger"]["form"]["fields"][1]
    assert second_field["id"] == "detailed_reason"
    assert second_field["condition"] is not None
    assert second_field["condition"]["type"] == "SINGLE_FIELD_VALUE"
    assert (
        second_field["condition"]["singleFieldValueCondition"]["field"]
        == "approval_type"
    )
    assert second_field["condition"]["singleFieldValueCondition"]["values"] == [
        "ADVANCED"
    ]
    assert (
        second_field["condition"]["singleFieldValueCondition"]["condition"] == "EQUAL"
    )
    assert second_field["condition"]["singleFieldValueCondition"]["negated"] is False

    # Track the created workflow for cleanup
    CREATED_WORKFLOW_URNS.append(workflow["urn"])

    print(f"✓ Successfully created workflow with condition field: {workflow['urn']}")
