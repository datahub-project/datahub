"""Tests for description management tools."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.descriptions import update_description


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.client.execute_graphql = Mock()
    return mock


# Replace operation tests


def test_update_description_replace_container(mock_client):
    """Test replacing description for a container (entity-level)."""
    description = "Production data warehouse containing customer data"
    entity_urn = "urn:li:container:12345"

    # Mock successful response
    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=description
        )

    assert result["success"] is True
    assert result["urn"] == entity_urn
    assert result["column_path"] is None
    assert "updated successfully" in result["message"]

    # Verify GraphQL was called once
    assert mock_client._graph.execute_graphql.call_count == 1


def test_update_description_replace_column(mock_client):
    """Test replacing description for a specific column."""
    description = "User's primary email address"
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "email"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn,
            operation="replace",
            description=description,
            column_path=column_path,
        )

    assert result["success"] is True
    assert result["urn"] == entity_urn
    assert result["column_path"] == column_path

    # Verify subResource fields are set for column-level descriptions
    call_args = mock_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["subResource"] == "email"
    assert call_args.kwargs["variables"]["input"]["subResourceType"] == "DATASET_FIELD"


def test_update_description_replace_with_markdown(mock_client):
    """Test replacing description with markdown formatting."""
    description = "# Production Container\n\nThis container contains **critical** data:\n- Databases\n- Tables\n- Views"
    entity_urn = "urn:li:container:prod-warehouse"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=description
        )

    assert result["success"] is True
    # Verify markdown is passed through unchanged
    call_args = mock_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == description


# Append operation tests


def test_update_description_append_to_existing_container(mock_client):
    """Test appending to existing container description."""
    entity_urn = "urn:li:container:12345"
    existing_description = "Data warehouse"
    append_text = "\n\n**Note:** This is the production environment."

    # Mock getEntity query response with existing description
    mock_client._graph.execute_graphql.side_effect = [
        # First call: getEntity
        {
            "entity": {
                "editableProperties": {"description": existing_description},
            }
        },
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify both getEntity and updateDescription were called
    assert mock_client._graph.execute_graphql.call_count == 2

    # Verify the final description is the concatenation
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_empty_container(mock_client):
    """Test appending when existing container description is empty."""
    entity_urn = "urn:li:container:12345"
    append_text = "New container description"

    # Mock getEntity query response with empty description
    mock_client._graph.execute_graphql.side_effect = [
        # First call: getEntity with empty description
        {"entity": {"editableProperties": {"description": ""}}},
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify the final description is just the append text
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == append_text


def test_update_description_append_to_column(mock_client):
    """Test appending to column-level description."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "email"
    existing_description = "Email field"
    append_text = " (PII)"

    # Mock getEntity query response with existing column description
    mock_client._graph.execute_graphql.side_effect = [
        # First call: getEntity
        {
            "entity": {
                "schemaMetadata": {
                    "fields": [
                        {"fieldPath": "email", "description": existing_description}
                    ]
                }
            }
        },
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn,
            operation="append",
            description=append_text,
            column_path=column_path,
        )

    assert result["success"] is True

    # Verify the final description includes both parts
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


# Remove operation tests


def test_update_description_remove_from_container(mock_client):
    """Test removing description from a container."""
    entity_urn = "urn:li:container:old-warehouse"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(entity_urn=entity_urn, operation="remove")
    assert result["success"] is True
    assert "removed successfully" in result["message"]

    # Verify empty description was sent
    call_args = mock_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == ""


def test_update_description_remove_from_column(mock_client):
    """Test removing description from a specific column."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "old_field"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn,
            operation="remove",
            column_path=column_path,
        )

    assert result["success"] is True

    # Verify subResource fields are set for column-level
    call_args = mock_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["subResource"] == "old_field"


# Validation tests


def test_update_description_empty_entity_urn(mock_client):
    """Test that empty entity_urn raises ValueError."""
    with pytest.raises(ValueError, match="entity_urn cannot be empty"):
        with DataHubContext(mock_client):
            update_description(entity_urn="", operation="replace", description="Test")


def test_update_description_replace_without_description(mock_client):
    """Test that replace operation requires description."""
    entity_urn = "urn:li:container:test"

    with pytest.raises(
        ValueError, match="description is required for 'replace' operation"
    ):
        with DataHubContext(mock_client):
            update_description(entity_urn=entity_urn, operation="replace")


def test_update_description_append_without_description(mock_client):
    """Test that append operation requires description."""
    entity_urn = "urn:li:container:test"

    with pytest.raises(
        ValueError, match="description is required for 'append' operation"
    ):
        with DataHubContext(mock_client):
            update_description(entity_urn=entity_urn, operation="append")


def test_update_description_invalid_operation(mock_client):
    """Test that invalid operation raises ValueError."""
    entity_urn = "urn:li:container:test"

    # This will be caught at type-checking time, but test runtime behavior
    with pytest.raises(ValueError, match="Invalid operation"):
        with DataHubContext(mock_client):
            update_description(
                entity_urn=entity_urn,
                operation="invalid",  # type: ignore
                description="Test",
            )


# Error handling tests


def test_update_description_mutation_returns_false(mock_client):
    """Test handling when mutation returns false."""
    description = "Test description"
    entity_urn = "urn:li:container:test"

    # Mutation returns false
    mock_client._graph.execute_graphql.return_value = {"updateDescription": False}

    with pytest.raises(RuntimeError, match="Failed to update description"):
        with DataHubContext(mock_client):
            update_description(
                entity_urn=entity_urn,
                operation="replace",
                description=description,
            )


def test_update_description_graphql_exception(mock_client):
    """Test handling of GraphQL execution errors."""
    description = "Test"
    entity_urn = "urn:li:container:test"

    # Mock GraphQL exception
    mock_client._graph.execute_graphql.side_effect = Exception("GraphQL error")

    with pytest.raises(RuntimeError, match="GraphQL error"):
        with DataHubContext(mock_client):
            update_description(
                entity_urn=entity_urn,
                operation="replace",
                description=description,
            )


def test_update_description_append_fetch_failure(mock_client):
    """Test that append continues with empty description if fetch fails."""
    entity_urn = "urn:li:container:test"
    append_text = "New text"

    # Mock getEntity failure, then successful updateDescription
    mock_client._graph.execute_graphql.side_effect = [
        Exception("Fetch failed"),
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    # Should still succeed, treating existing description as empty
    assert result["success"] is True

    # Verify the final description is just the append text (no existing description)
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == append_text


# Tests for new entity types (Tag, GlossaryTerm, GlossaryNode, Domain)


def test_update_description_append_to_tag(mock_client):
    """Test appending to Tag description (uses properties field)."""
    entity_urn = "urn:li:tag:PII"
    existing_description = "Personally Identifiable Information"
    append_text = " - Requires special handling"

    # Mock getEntity query response with Tag properties
    mock_client._graph.execute_graphql.side_effect = [
        # First call: getEntity with properties field (not editableProperties)
        {"entity": {"properties": {"description": existing_description}}},
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify the final description is the concatenation
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_glossary_term(mock_client):
    """Test appending to GlossaryTerm description (uses properties field)."""
    entity_urn = "urn:li:glossaryTerm:CustomerData"
    existing_description = "Data related to customers"
    append_text = "\n\nIncludes: names, emails, phone numbers"

    mock_client._graph.execute_graphql.side_effect = [
        {"entity": {"properties": {"description": existing_description}}},
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_fallback_to_properties(mock_client):
    """Test that code falls back to properties field when editableProperties is empty."""
    entity_urn = "urn:li:tag:TestTag"
    existing_description = "Test tag description"
    append_text = " - additional info"

    # Mock response with empty editableProperties but populated properties
    mock_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "editableProperties": {"description": ""},
                "properties": {"description": existing_description},
            }
        },
        {"updateDescription": True},
    ]

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify it used the properties field description
    update_call = mock_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_replace_tag(mock_client):
    """Test replacing Tag description."""
    entity_urn = "urn:li:tag:Deprecated"
    new_description = "This tag marks deprecated assets"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(
            entity_urn=entity_urn,
            operation="replace",
            description=new_description,
        )

    assert result["success"] is True
    assert "updated successfully" in result["message"]


def test_update_description_remove_glossary_term(mock_client):
    """Test removing GlossaryTerm description."""
    entity_urn = "urn:li:glossaryTerm:OldTerm"

    mock_client._graph.execute_graphql.return_value = {"updateDescription": True}

    with DataHubContext(mock_client):
        result = update_description(entity_urn=entity_urn, operation="remove")
    assert result["success"] is True
    assert "removed successfully" in result["message"]

    # Verify empty description was sent
    call_args = mock_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == ""
