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
    print("ingesting action workflow test data for delete tests")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    yield
    print("removing action workflow test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_workflows_data.json")


def test_delete_action_workflow_success(auth_session):
    """Test successfully deleting an existing action workflow"""

    # Use dedicated deletion test workflow from test data
    workflow_urn = "urn:li:actionWorkflow:delete-test-workflow-1"

    # Verify the workflow exists by listing workflows
    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            total
            workflows {
                urn
                name
                category
            }
        }
    }
    """

    list_variables = {"input": {"start": 0, "count": 20}}

    # Verify workflow exists before deletion
    response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    workflows_before = response["data"]["listActionWorkflows"]["workflows"]
    workflow_urns_before = [w["urn"] for w in workflows_before]
    assert workflow_urn in workflow_urns_before
    print(f"✓ Verified workflow exists before deletion: {workflow_urn}")

    # Now delete the workflow
    delete_query = """
    mutation deleteActionWorkflow($input: DeleteActionWorkflowInput!) {
        deleteActionWorkflow(input: $input)
    }
    """

    delete_variables = {"input": {"urn": workflow_urn}}

    response = execute_gql_with_retry(auth_session, delete_query, delete_variables)

    # Should succeed
    assert "errors" not in response
    assert response["data"]["deleteActionWorkflow"] is True
    print(f"✓ Successfully deleted workflow: {workflow_urn}")

    # Verify workflow no longer exists by listing workflows again
    response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in response
    assert response["data"]["listActionWorkflows"]

    workflows_after = response["data"]["listActionWorkflows"]["workflows"]
    workflow_urns_after = [w["urn"] for w in workflows_after]
    assert workflow_urn not in workflow_urns_after
    print("✓ Verified workflow no longer exists after deletion")

    # Verify the count decreased by 1
    total_before = len(workflows_before)
    total_after = len(workflows_after)
    assert total_after == total_before - 1
    print(f"✓ Workflow count decreased from {total_before} to {total_after}")


def test_delete_action_workflow_not_found(auth_session):
    """Test error handling when trying to delete a non-existent workflow"""

    non_existent_urn = "urn:li:actionWorkflow:does-not-exist"

    delete_query = """
    mutation deleteActionWorkflow($input: DeleteActionWorkflowInput!) {
        deleteActionWorkflow(input: $input)
    }
    """

    delete_variables = {"input": {"urn": non_existent_urn}}

    response = execute_gql_with_retry(auth_session, delete_query, delete_variables)

    # Should return an error since the workflow doesn't exist
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(f"✓ Correctly returned error for non-existent workflow: {non_existent_urn}")


def test_delete_action_workflow_invalid_urn_format(auth_session):
    """Test error handling for invalid URN format"""

    invalid_urn = "invalid-urn-format"

    delete_query = """
    mutation deleteActionWorkflow($input: DeleteActionWorkflowInput!) {
        deleteActionWorkflow(input: $input)
    }
    """

    delete_variables = {"input": {"urn": invalid_urn}}

    response = execute_gql_with_retry(auth_session, delete_query, delete_variables)

    # Should return an error due to invalid URN format
    assert "errors" in response
    assert len(response["errors"]) > 0
    print(f"✓ Correctly returned error for invalid URN format: {invalid_urn}")


def test_delete_action_workflow_multiple_deletions(auth_session):
    """Test deleting multiple workflows to ensure the operation works correctly for different workflows"""

    # Use dedicated deletion test workflows from test data
    workflow_urns = [
        "urn:li:actionWorkflow:delete-test-workflow-2",
        "urn:li:actionWorkflow:delete-test-workflow-3",
    ]

    # Get initial workflow count
    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            total
            workflows {
                urn
                name
                category
            }
        }
    }
    """

    list_variables = {"input": {"start": 0, "count": 20}}

    response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in response
    workflows_initial = response["data"]["listActionWorkflows"]["workflows"]
    initial_urns = [w["urn"] for w in workflows_initial]
    initial_count = len(workflows_initial)

    # Verify all target workflows exist
    for urn in workflow_urns:
        assert urn in initial_urns
    print("✓ Verified all target workflows exist before deletion")

    # Delete each workflow
    delete_query = """
    mutation deleteActionWorkflow($input: DeleteActionWorkflowInput!) {
        deleteActionWorkflow(input: $input)
    }
    """

    deleted_count = 0
    for workflow_urn in workflow_urns:
        delete_variables = {"input": {"urn": workflow_urn}}

        response = execute_gql_with_retry(auth_session, delete_query, delete_variables)
        assert "errors" not in response
        assert response["data"]["deleteActionWorkflow"] is True
        deleted_count += 1
        print(
            f"✓ Successfully deleted workflow {deleted_count}/{len(workflow_urns)}: {workflow_urn}"
        )

    # Verify all workflows are deleted
    response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in response
    workflows_final = response["data"]["listActionWorkflows"]["workflows"]
    final_urns = [w["urn"] for w in workflows_final]
    final_count = len(workflows_final)

    # Verify none of the deleted workflows exist
    for urn in workflow_urns:
        assert urn not in final_urns

    # Verify the count decreased by the number of deleted workflows
    assert final_count == initial_count - len(workflow_urns)
    print(
        f"✓ Verified all workflows deleted. Count decreased from {initial_count} to {final_count}"
    )


def test_delete_action_workflow_with_special_characters(auth_session):
    """Test deleting workflow that was created with special characters in the name"""

    # First create a workflow with special characters, then delete it
    # This test creates its own workflow since it's testing a specific edge case
    import time

    workflow_name = (
        f"Test Workflow with Special Characters: #@$%^&*()! {int(time.time())}"
    )

    # Create the workflow first
    upsert_query = """
    mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
        upsertActionWorkflow(input: $input) {
            urn
            name
            category
        }
    }
    """

    upsert_variables = {
        "input": {
            "name": workflow_name,
            "category": "ACCESS",
            "description": "Test workflow with special characters for deletion testing",
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
                    "actors": {
                        "userUrns": [get_root_urn()],
                        "groupUrns": [],
                        "roleUrns": [],
                    },
                }
            ],
        }
    }

    response = execute_gql_with_retry(auth_session, upsert_query, upsert_variables)
    assert "errors" not in response
    created_workflow = response["data"]["upsertActionWorkflow"]
    created_urn = created_workflow["urn"]
    print(f"✓ Created workflow with special characters: {created_urn}")

    # Now delete it
    delete_query = """
    mutation deleteActionWorkflow($input: DeleteActionWorkflowInput!) {
        deleteActionWorkflow(input: $input)
    }
    """

    delete_variables = {"input": {"urn": created_urn}}

    response = execute_gql_with_retry(auth_session, delete_query, delete_variables)
    assert "errors" not in response
    assert response["data"]["deleteActionWorkflow"] is True
    print(f"✓ Successfully deleted workflow with special characters: {created_urn}")

    # Verify it's deleted by trying to list and finding it's not there
    list_query = """
    query listActionWorkflows($input: ListActionWorkflowsInput!) {
        listActionWorkflows(input: $input) {
            workflows {
                urn
                name
            }
        }
    }
    """

    list_variables = {
        "input": {
            "start": 0,
            "count": 50,  # Higher count to make sure we catch it if it exists
        }
    }

    response = execute_gql_with_retry(auth_session, list_query, list_variables)
    assert "errors" not in response
    workflows = response["data"]["listActionWorkflows"]["workflows"]
    workflow_urns = [w["urn"] for w in workflows]
    assert created_urn not in workflow_urns
    print("✓ Verified workflow with special characters is no longer listed")
