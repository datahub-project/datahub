"""Tests for owner management tools."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.owners import (
    OwnershipType,
    add_owners,
    remove_owners,
)


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient with a mock execute_graphql."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    return mock


# ===== Tests for add_owners =====


def test_add_owners_to_multiple_datasets(mock_client):
    """Test adding owners to multiple datasets."""
    owner_urns = [
        "urn:li:corpuser:john.doe",
        "urn:li:corpGroup:data-engineering",
    ]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
    ]

    # Mock validation response showing owners exist
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": owner_urns[0], "type": "CORP_USER", "username": "john.doe"},
                {
                    "urn": owner_urns[1],
                    "type": "CORP_GROUP",
                    "name": "data-engineering",
                },
            ]
        },
        {"batchAddOwners": True},
    ]

    with DataHubContext(mock_client):
        result = add_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )
    assert result["success"] is True
    assert "Successfully added 2 owner(s) to 2 entit(ies)" in result["message"]

    # Verify GraphQL was called twice: once for validation, once for mutation
    assert mock_client._graph.execute_graphql.call_count == 2

    # Check the mutation call (second call)
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert len(variables["input"]["owners"]) == 2
    assert variables["input"]["owners"][0]["ownerUrn"] == owner_urns[0]
    assert variables["input"]["owners"][0]["ownerEntityType"] == "CORP_USER"
    assert variables["input"]["owners"][1]["ownerUrn"] == owner_urns[1]
    assert variables["input"]["owners"][1]["ownerEntityType"] == "CORP_GROUP"
    assert len(variables["input"]["resources"]) == 2
    assert variables["input"]["resources"][0]["resourceUrn"] == entity_urns[0]
    assert variables["input"]["resources"][1]["resourceUrn"] == entity_urns[1]


def test_add_owners_with_technical_owner_type(mock_client):
    """Test adding owners with technical owner type."""
    owner_urns = ["urn:li:corpuser:john.doe"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    # Mock validation response
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": owner_urns[0], "type": "CORP_USER", "username": "john.doe"}
            ]
        },
        {"batchAddOwners": True},
    ]

    with DataHubContext(mock_client):
        result = add_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )

    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # Verify technical owner ownership type is applied
    assert (
        variables["input"]["owners"][0]["ownershipTypeUrn"]
        == "urn:li:ownershipType:__system__technical_owner"
    )
    assert (
        variables["input"]["ownershipTypeUrn"]
        == "urn:li:ownershipType:__system__technical_owner"
    )


def test_add_owners_to_mixed_entity_types(mock_client):
    """Test adding owners to different entity types (dataset, dashboard)."""
    owner_urns = ["urn:li:corpuser:data.steward"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:looker,sales_dashboard,PROD)",
    ]

    # Mock validation response
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": owner_urns[0], "type": "CORP_USER", "username": "data.steward"}
            ]
        },
        {"batchAddOwners": True},
    ]

    with DataHubContext(mock_client):
        result = add_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )
    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert len(variables["input"]["resources"]) == 2
    assert variables["input"]["resources"][0]["resourceUrn"] == entity_urns[0]
    assert variables["input"]["resources"][1]["resourceUrn"] == entity_urns[1]


def test_add_owners_with_nonexistent_owner(mock_client):
    """Test that adding nonexistent owner URN returns error."""
    owner_urns = ["urn:li:corpuser:nonexistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (owner doesn't exist)
    mock_client._graph.execute_graphql.return_value = {"entities": []}

    with pytest.raises(ValueError, match="do not exist in DataHub"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_add_owners_with_invalid_owner_type(mock_client):
    """Test that adding a URN that is not a CorpUser or CorpGroup returns error."""
    owner_urns = ["urn:li:dataset:not_an_owner"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a Dataset entity instead of CorpUser/CorpGroup
    mock_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": owner_urns[0], "type": "DATASET"}]
    }

    with pytest.raises(ValueError, match="not valid owner entities"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_add_owners_empty_owner_urns(mock_client):
    """Test that empty owner_urns raises ValueError."""
    with pytest.raises(ValueError, match="owner_urns cannot be empty"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=[],
                entity_urns=["urn:li:dataset:test"],
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_add_owners_empty_entity_urns(mock_client):
    """Test that empty entity_urns raises ValueError."""
    with pytest.raises(ValueError, match="entity_urns cannot be empty"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=["urn:li:corpuser:test"],
                entity_urns=[],
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_add_owners_mutation_returns_false(mock_client):
    """Test handling of mutation returning false."""
    owner_urns = ["urn:li:corpuser:test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation failure
    mock_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": owner_urns[0], "type": "CORP_USER", "username": "test"}]},
        {"batchAddOwners": False},
    ]

    with pytest.raises(RuntimeError, match="Failed to add owners"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_add_owners_graphql_exception(mock_client):
    """Test handling of GraphQL execution exception."""
    owner_urns = ["urn:li:corpuser:test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation raises exception
    mock_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": owner_urns[0], "type": "CORP_USER", "username": "test"}]},
        Exception("GraphQL error"),
    ]

    with pytest.raises(RuntimeError, match="Error add owners"):
        with DataHubContext(mock_client):
            add_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


# ===== Tests for remove_owners =====


def test_remove_owners_from_multiple_datasets(mock_client):
    """Test removing owners from multiple datasets."""
    owner_urns = [
        "urn:li:corpuser:former.employee",
        "urn:li:corpGroup:old-team",
    ]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
    ]

    # Mock validation response
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {
                    "urn": owner_urns[0],
                    "type": "CORP_USER",
                    "username": "former.employee",
                },
                {"urn": owner_urns[1], "type": "CORP_GROUP", "name": "old-team"},
            ]
        },
        {"batchRemoveOwners": True},
    ]

    with DataHubContext(mock_client):
        result = remove_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )
    assert result["success"] is True
    assert "Successfully removed 2 owner(s) from 2 entit(ies)" in result["message"]

    # Verify GraphQL was called twice: once for validation, once for mutation
    assert mock_client._graph.execute_graphql.call_count == 2

    # Check the mutation call (second call)
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "batchRemoveOwners"
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["ownerUrns"] == owner_urns
    assert len(variables["input"]["resources"]) == 2


def test_remove_owners_with_ownership_type(mock_client):
    """Test removing owners with a specific ownership type."""
    owner_urns = ["urn:li:corpuser:old.owner"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    # Mock validation response
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": owner_urns[0], "type": "CORP_USER", "username": "old.owner"}
            ]
        },
        {"batchRemoveOwners": True},
    ]

    with DataHubContext(mock_client):
        result = remove_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )

    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # Verify ownership type is included
    expected_urn = "urn:li:ownershipType:__system__technical_owner"
    assert variables["input"]["ownershipTypeUrn"] == expected_urn


def test_remove_owners_from_mixed_entity_types(mock_client):
    """Test removing owners from different entity types."""
    owner_urns = ["urn:li:corpuser:temp.owner"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:looker,temp_dashboard,PROD)",
    ]

    # Mock validation response
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": owner_urns[0], "type": "CORP_USER", "username": "temp.owner"}
            ]
        },
        {"batchRemoveOwners": True},
    ]

    with DataHubContext(mock_client):
        result = remove_owners(
            owner_urns=owner_urns,
            entity_urns=entity_urns,
            ownership_type=OwnershipType.TECHNICAL_OWNER,
        )
    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert len(variables["input"]["resources"]) == 2
    assert variables["input"]["resources"][0]["resourceUrn"] == entity_urns[0]
    assert variables["input"]["resources"][1]["resourceUrn"] == entity_urns[1]


def test_remove_owners_with_nonexistent_owner(mock_client):
    """Test that removing nonexistent owner URN returns error."""
    owner_urns = ["urn:li:corpuser:nonexistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (owner doesn't exist)
    mock_client._graph.execute_graphql.return_value = {"entities": []}

    with pytest.raises(ValueError, match="do not exist in DataHub"):
        with DataHubContext(mock_client):
            remove_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_remove_owners_with_invalid_owner_type(mock_client):
    """Test that removing a URN that is not a CorpUser or CorpGroup returns error."""
    owner_urns = ["urn:li:dataset:not_an_owner"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a Dataset entity instead of CorpUser/CorpGroup
    mock_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": owner_urns[0], "type": "DATASET"}]
    }

    with pytest.raises(ValueError, match="not valid owner entities"):
        with DataHubContext(mock_client):
            remove_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_remove_owners_empty_owner_urns(mock_client):
    """Test that empty owner_urns raises ValueError."""
    with pytest.raises(ValueError, match="owner_urns cannot be empty"):
        with DataHubContext(mock_client):
            remove_owners(
                owner_urns=[],
                entity_urns=["urn:li:dataset:test"],
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_remove_owners_empty_entity_urns(mock_client):
    """Test that empty entity_urns raises ValueError."""
    with pytest.raises(ValueError, match="entity_urns cannot be empty"):
        with DataHubContext(mock_client):
            remove_owners(
                owner_urns=["urn:li:corpuser:test"],
                entity_urns=[],
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_remove_owners_mutation_returns_false(mock_client):
    """Test handling of mutation returning false."""
    owner_urns = ["urn:li:corpuser:test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation failure
    mock_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": owner_urns[0], "type": "CORP_USER", "username": "test"}]},
        {"batchRemoveOwners": False},
    ]

    with pytest.raises(RuntimeError, match="Failed to remove owners"):
        with DataHubContext(mock_client):
            remove_owners(
                owner_urns=owner_urns,
                entity_urns=entity_urns,
                ownership_type=OwnershipType.TECHNICAL_OWNER,
            )


def test_remove_owners_graphql_exception(mock_client):
    """Test handling of GraphQL execution exception."""
    owner_urns = ["urn:li:corpuser:test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation raises exception
    mock_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": owner_urns[0], "type": "CORP_USER", "username": "test"}]},
        Exception("GraphQL error"),
    ]

    with pytest.raises(RuntimeError, match="Error remove owners"):
        with DataHubContext(mock_client):
            remove_owners(owner_urns=owner_urns, entity_urns=entity_urns)
