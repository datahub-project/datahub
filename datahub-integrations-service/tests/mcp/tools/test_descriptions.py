"""Tests for description management tools."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.descriptions import update_description


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client."""
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


# Replace operation tests


def test_update_description_replace_container(mock_datahub_client):
    """Test replacing description for a container (entity-level)."""
    description = "Production data warehouse containing customer data"
    entity_urn = "urn:li:container:12345"

    # Mock successful response
    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=description
        )

    assert result["success"] is True
    assert result["urn"] == entity_urn
    assert result["column_path"] is None
    assert "updated successfully" in result["message"]

    # Verify GraphQL was called once
    assert mock_datahub_client._graph.execute_graphql.call_count == 1


def test_update_description_replace_column(mock_datahub_client):
    """Test replacing description for a specific column."""
    description = "User's primary email address"
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "email"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
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
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["subResource"] == "email"
    assert call_args.kwargs["variables"]["input"]["subResourceType"] == "DATASET_FIELD"


def test_update_description_replace_with_markdown(mock_datahub_client):
    """Test replacing description with markdown formatting."""
    description = "# Production Container\n\nThis container contains **critical** data:\n- Databases\n- Tables\n- Views"
    entity_urn = "urn:li:container:prod-warehouse"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=description
        )

    assert result["success"] is True
    # Verify markdown is passed through unchanged
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == description


# Append operation tests


def test_update_description_append_to_existing_container(mock_datahub_client):
    """Test appending to existing container description."""
    entity_urn = "urn:li:container:12345"
    existing_description = "Data warehouse"
    append_text = "\n\n**Note:** This is the production environment."

    # Mock getEntity query response with existing description
    mock_datahub_client._graph.execute_graphql.side_effect = [
        # First call: getEntity
        {
            "entity": {
                "editableProperties": {"description": existing_description},
            }
        },
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify both getEntity and updateDescription were called
    assert mock_datahub_client._graph.execute_graphql.call_count == 2

    # Verify the final description is the concatenation
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_empty_container(mock_datahub_client):
    """Test appending when existing container description is empty."""
    entity_urn = "urn:li:container:12345"
    append_text = "New container description"

    # Mock getEntity query response with empty description
    mock_datahub_client._graph.execute_graphql.side_effect = [
        # First call: getEntity with empty description
        {"entity": {"editableProperties": {"description": ""}}},
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify the final description is just the append text
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == append_text


def test_update_description_append_to_column(mock_datahub_client):
    """Test appending to column-level description."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "email"
    existing_description = "Email field"
    append_text = " (PII)"

    # Mock getEntity query response with existing column description
    mock_datahub_client._graph.execute_graphql.side_effect = [
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

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn,
            operation="append",
            description=append_text,
            column_path=column_path,
        )

    assert result["success"] is True

    # Verify the final description includes both parts
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


# Remove operation tests


def test_update_description_remove_from_container(mock_datahub_client):
    """Test removing description from a container."""
    entity_urn = "urn:li:container:old-warehouse"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(entity_urn=entity_urn, operation="remove")

    assert result["success"] is True
    assert "removed successfully" in result["message"]

    # Verify empty description was sent
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == ""


def test_update_description_remove_from_column(mock_datahub_client):
    """Test removing description from a specific column."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    column_path = "old_field"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="remove", column_path=column_path
        )

    assert result["success"] is True

    # Verify subResource fields are set for column-level
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["subResource"] == "old_field"


# Validation tests


def test_update_description_empty_entity_urn(mock_datahub_client):
    """Test that empty entity_urn raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urn cannot be empty"):
            update_description(entity_urn="", operation="replace", description="Test")


def test_update_description_replace_without_description(mock_datahub_client):
    """Test that replace operation requires description."""
    entity_urn = "urn:li:container:test"

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            ValueError, match="description is required for 'replace' operation"
        ):
            update_description(entity_urn=entity_urn, operation="replace")


def test_update_description_append_without_description(mock_datahub_client):
    """Test that append operation requires description."""
    entity_urn = "urn:li:container:test"

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            ValueError, match="description is required for 'append' operation"
        ):
            update_description(entity_urn=entity_urn, operation="append")


def test_update_description_invalid_operation(mock_datahub_client):
    """Test that invalid operation raises ValueError."""
    entity_urn = "urn:li:container:test"

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        # This will be caught at type-checking time, but test runtime behavior
        with pytest.raises(ValueError, match="Invalid operation"):
            update_description(
                entity_urn=entity_urn,
                operation="invalid",  # type: ignore
                description="Test",
            )


# Error handling tests


def test_update_description_mutation_returns_false(mock_datahub_client):
    """Test handling when mutation returns false."""
    description = "Test description"
    entity_urn = "urn:li:container:test"

    # Mutation returns false
    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": False
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ update\ description"):
            update_description(
                entity_urn=entity_urn, operation="replace", description=description
            )


def test_update_description_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution errors."""
    description = "Test"
    entity_urn = "urn:li:container:test"

    # Mock GraphQL exception
    mock_datahub_client._graph.execute_graphql.side_effect = Exception("GraphQL error")

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="GraphQL\ error"):
            update_description(
                entity_urn=entity_urn, operation="replace", description=description
            )


def test_update_description_append_fetch_failure(mock_datahub_client):
    """Test that append continues with empty description if fetch fails."""
    entity_urn = "urn:li:container:test"
    append_text = "New text"

    # Mock getEntity failure, then successful updateDescription
    mock_datahub_client._graph.execute_graphql.side_effect = [
        Exception("Fetch failed"),
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    # Should still succeed, treating existing description as empty
    assert result["success"] is True

    # Verify the final description is just the append text (no existing description)
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == append_text


def test_update_description_operation_succeeds(mock_datahub_client):
    """Test that success is True when operation succeeds."""
    description = "Test"
    entity_urn = "urn:li:container:test"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=description
        )

    assert result["success"] is True
    assert "updated successfully" in result["message"]


# Tests for new entity types (Tag, GlossaryTerm, GlossaryNode, Domain)


def test_update_description_append_to_tag(mock_datahub_client):
    """Test appending to Tag description (uses properties field)."""
    entity_urn = "urn:li:tag:PII"
    existing_description = "Personally Identifiable Information"
    append_text = " - Requires special handling"

    # Mock getEntity query response with Tag properties
    mock_datahub_client._graph.execute_graphql.side_effect = [
        # First call: getEntity with properties field (not editableProperties)
        {"entity": {"properties": {"description": existing_description}}},
        # Second call: updateDescription
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify the final description is the concatenation
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_glossary_term(mock_datahub_client):
    """Test appending to GlossaryTerm description (uses properties field)."""
    entity_urn = "urn:li:glossaryTerm:CustomerData"
    existing_description = "Data related to customers"
    append_text = "\n\nIncludes: names, emails, phone numbers"

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"properties": {"description": existing_description}}},
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_glossary_node(mock_datahub_client):
    """Test appending to GlossaryNode description (uses properties field)."""
    entity_urn = "urn:li:glossaryNode:DataGovernance"
    existing_description = "Data Governance Terms"
    append_text = "\n\nOwned by Compliance team"

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"properties": {"description": existing_description}}},
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_append_to_domain(mock_datahub_client):
    """Test appending to Domain description (uses properties field)."""
    entity_urn = "urn:li:domain:marketing"
    existing_description = "Marketing Domain"
    append_text = "\n\nContains all marketing-related datasets"

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"properties": {"description": existing_description}}},
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_fallback_to_properties(mock_datahub_client):
    """Test that code falls back to properties field when editableProperties is empty."""
    entity_urn = "urn:li:tag:TestTag"
    existing_description = "Test tag description"
    append_text = " - additional info"

    # Mock response with empty editableProperties but populated properties
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "editableProperties": {"description": ""},
                "properties": {"description": existing_description},
            }
        },
        {"updateDescription": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="append", description=append_text
        )

    assert result["success"] is True

    # Verify it used the properties field description
    update_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    final_description = update_call.kwargs["variables"]["input"]["description"]
    assert final_description == existing_description + append_text


def test_update_description_replace_tag(mock_datahub_client):
    """Test replacing Tag description."""
    entity_urn = "urn:li:tag:Deprecated"
    new_description = "This tag marks deprecated assets"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(
            entity_urn=entity_urn, operation="replace", description=new_description
        )

    assert result["success"] is True
    assert "updated successfully" in result["message"]


def test_update_description_remove_glossary_term(mock_datahub_client):
    """Test removing GlossaryTerm description."""
    entity_urn = "urn:li:glossaryTerm:OldTerm"

    mock_datahub_client._graph.execute_graphql.return_value = {
        "updateDescription": True
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_description(entity_urn=entity_urn, operation="remove")

    assert result["success"] is True
    assert "removed successfully" in result["message"]

    # Verify empty description was sent
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["description"] == ""
