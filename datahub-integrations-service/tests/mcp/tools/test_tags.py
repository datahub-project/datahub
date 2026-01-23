from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.tags import add_tags, remove_tags


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_graph = Mock()
    mock_client._graph = mock_graph
    return mock_client


def test_add_tags_to_multiple_datasets(mock_datahub_client):
    """Test adding tags to multiple datasets (entity-level)."""
    tag_urns = ["urn:li:tag:PII", "urn:li:tag:Sensitive"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
    ]

    # Mock validation response showing tags exist
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}},
                {
                    "urn": tag_urns[1],
                    "type": "TAG",
                    "properties": {"name": "Sensitive"},
                },
            ]
        },
        {"batchAddTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_tags(tag_urns=tag_urns, entity_urns=entity_urns)

    assert result["success"] is True
    assert "Successfully added 2 tag(s) to 2 entit(ies)" in result["message"]

    # Verify GraphQL was called twice: once for validation, once for mutation
    assert mock_datahub_client._graph.execute_graphql.call_count == 2

    # Check the mutation call (second call)
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["tagUrns"] == tag_urns
    assert len(variables["input"]["resources"]) == 2
    assert variables["input"]["resources"][0]["resourceUrn"] == entity_urns[0]
    assert "subResource" not in variables["input"]["resources"][0]


def test_add_tags_to_columns(mock_datahub_client):
    """Test adding tags to specific columns (column-level)."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = ["email", "phone_number"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchAddTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    assert len(resources) == 2
    assert resources[0]["subResource"] == "email"
    assert resources[0]["subResourceType"] == "DATASET_FIELD"
    assert resources[1]["subResource"] == "phone_number"
    assert resources[1]["subResourceType"] == "DATASET_FIELD"


def test_add_tags_mixed_entity_and_column_level(mock_datahub_client):
    """Test adding tags with mixed entity-level and column-level targets."""
    tag_urns = ["urn:li:tag:Deprecated"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = [None, "deprecated_column"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {
                    "urn": tag_urns[0],
                    "type": "TAG",
                    "properties": {"name": "Deprecated"},
                }
            ]
        },
        {"batchAddTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    # First resource should be entity-level (no subResource)
    assert "subResource" not in resources[0]
    assert "subResourceType" not in resources[0]

    # Second resource should be column-level
    assert resources[1]["subResource"] == "deprecated_column"
    assert resources[1]["subResourceType"] == "DATASET_FIELD"


def test_add_tags_empty_tag_urns(mock_datahub_client):
    """Test that empty tag_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="tag_urns cannot be empty"):
            add_tags(tag_urns=[], entity_urns=["urn:li:dataset:test"])


def test_add_tags_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            add_tags(tag_urns=["urn:li:tag:PII"], entity_urns=[])


def test_add_tags_mismatched_column_paths_length(mock_datahub_client):
    """Test that mismatched column_paths length raises ValueError."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test1", "urn:li:dataset:test2"]
    column_paths = ["column1"]  # Only 1 subresource for 2 resources

    # Mock validation success (validation happens before length check)
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="column_paths length.*must match"):
            add_tags(
                tag_urns=tag_urns,
                entity_urns=entity_urns,
                column_paths=column_paths,
            )


def test_add_tags_graphql_failure(mock_datahub_client):
    """Test handling of GraphQL mutation returning false."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, then mutation failure
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchAddTags": False},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ add\ tags"):
            add_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_add_tags_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL exceptions during mutation."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, then mutation exception
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        Exception("Network error"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error add\ tags"):
            add_tags(tag_urns=tag_urns, entity_urns=entity_urns)


# Tests for remove_tags


def test_remove_tags_from_multiple_datasets(mock_datahub_client):
    """Test removing tags from multiple datasets (entity-level)."""
    tag_urns = ["urn:li:tag:Deprecated", "urn:li:tag:Legacy"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {
                    "urn": tag_urns[0],
                    "type": "TAG",
                    "properties": {"name": "Deprecated"},
                },
                {"urn": tag_urns[1], "type": "TAG", "properties": {"name": "Legacy"}},
            ]
        },
        {"batchRemoveTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_tags(tag_urns=tag_urns, entity_urns=entity_urns)

    assert result["success"] is True
    assert "Successfully removed 2 tag(s) from 2 entit(ies)" in result["message"]

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["tagUrns"] == tag_urns
    assert len(variables["input"]["resources"]) == 2


def test_remove_tags_from_columns(mock_datahub_client):
    """Test removing tags from specific columns (column-level)."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = ["old_email_field", "deprecated_phone"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchRemoveTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    assert resources[0]["subResource"] == "old_email_field"
    assert resources[0]["subResourceType"] == "DATASET_FIELD"
    assert resources[1]["subResource"] == "deprecated_phone"


def test_remove_tags_mixed_entity_and_column_level(mock_datahub_client):
    """Test removing tags with mixed entity-level and column-level targets."""
    tag_urns = ["urn:li:tag:Experimental"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = [None, "test_column"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {
                    "urn": tag_urns[0],
                    "type": "TAG",
                    "properties": {"name": "Experimental"},
                }
            ]
        },
        {"batchRemoveTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    # First resource should be entity-level
    assert "subResource" not in resources[0]

    # Second resource should be column-level
    assert resources[1]["subResource"] == "test_column"
    assert resources[1]["subResourceType"] == "DATASET_FIELD"


def test_remove_tags_empty_tag_urns(mock_datahub_client):
    """Test that empty tag_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="tag_urns cannot be empty"):
            remove_tags(tag_urns=[], entity_urns=["urn:li:dataset:test"])


def test_remove_tags_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            remove_tags(tag_urns=["urn:li:tag:PII"], entity_urns=[])


def test_remove_tags_mismatched_column_paths_length(mock_datahub_client):
    """Test that mismatched column_paths length raises ValueError."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test1", "urn:li:dataset:test2"]
    column_paths = ["column1"]  # Only 1 subresource for 2 resources

    # Mock validation success (validation happens before length check)
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="column_paths length.*must match"):
            remove_tags(
                tag_urns=tag_urns,
                entity_urns=entity_urns,
                column_paths=column_paths,
            )


def test_remove_tags_graphql_failure(mock_datahub_client):
    """Test handling of GraphQL mutation returning false."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchRemoveTags": False},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ remove\ tags"):
            remove_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_remove_tags_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL exceptions during mutation."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        Exception("Authorization error"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error remove\ tags"):
            remove_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_add_tags_with_empty_string_subresource(mock_datahub_client):
    """Test that empty string subresource is treated as None (entity-level)."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]
    column_paths = [""]  # Empty string should be treated as entity-level

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchAddTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    # Empty string is falsy, so should not add subResource fields
    assert "subResource" not in resources[0]
    assert "subResourceType" not in resources[0]


def test_remove_tags_with_empty_string_subresource(mock_datahub_client):
    """Test that empty string subresource is treated as None (entity-level)."""
    tag_urns = ["urn:li:tag:PII"]
    entity_urns = ["urn:li:dataset:test"]
    column_paths = [""]  # Empty string should be treated as entity-level

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": tag_urns[0], "type": "TAG", "properties": {"name": "PII"}}
            ]
        },
        {"batchRemoveTags": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_tags(
            tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]
    resources = variables["input"]["resources"]

    # Empty string is falsy, so should not add subResource fields
    assert "subResource" not in resources[0]
    assert "subResourceType" not in resources[0]


def test_add_tags_with_nonexistent_tag(mock_datahub_client):
    """Test that adding nonexistent tag URN returns error."""
    tag_urns = ["urn:li:tag:NonExistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (tag doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entities": []}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="do\ not\ exist\ in\ DataHub"):
            add_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_add_tags_with_non_tag_urn(mock_datahub_client):
    """Test that adding non-tag URN returns error."""
    tag_urns = ["urn:li:dataset:not_a_tag"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a dataset entity instead of tag
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": tag_urns[0], "type": "DATASET"}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not\ tag\ entities"):
            add_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_remove_tags_with_nonexistent_tag(mock_datahub_client):
    """Test that removing nonexistent tag URN returns error."""
    tag_urns = ["urn:li:tag:NonExistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (tag doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entities": []}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="do\ not\ exist\ in\ DataHub"):
            remove_tags(tag_urns=tag_urns, entity_urns=entity_urns)


def test_remove_tags_with_non_tag_urn(mock_datahub_client):
    """Test that removing non-tag URN returns error."""
    tag_urns = ["urn:li:dataset:not_a_tag"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a dataset entity instead of tag
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": tag_urns[0], "type": "DATASET"}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not\ tag\ entities"):
            remove_tags(tag_urns=tag_urns, entity_urns=entity_urns)
