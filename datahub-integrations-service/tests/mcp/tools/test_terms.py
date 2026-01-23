"""Tests for terms management tools."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client with a mock graph."""
    client = MagicMock()
    client._graph = MagicMock()
    client._graph.execute_graphql = MagicMock()
    return client


# ===== Tests for add_glossary_terms =====


def test_add_glossary_terms_to_multiple_datasets(mock_datahub_client):
    """Test adding glossary terms to multiple datasets (entity-level)."""
    term_urns = [
        "urn:li:glossaryTerm:CustomerData",
        "urn:li:glossaryTerm:PersonalInformation",
    ]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
    ]

    # Mock validation response showing glossary terms exist
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "CustomerData"},
                {
                    "urn": term_urns[1],
                    "type": "GLOSSARY_TERM",
                    "name": "PersonalInformation",
                },
            ]
        },
        {"batchAddTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)

    assert result["success"] is True
    assert "Successfully added 2 glossary term(s) to 2 entit(ies)" in result["message"]

    # Verify GraphQL was called twice: once for validation, once for mutation
    assert mock_datahub_client._graph.execute_graphql.call_count == 2

    # Check the mutation call (second call)
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["termUrns"] == term_urns
    assert len(variables["input"]["resources"]) == 2
    assert variables["input"]["resources"][0]["resourceUrn"] == entity_urns[0]
    assert variables["input"]["resources"][1]["resourceUrn"] == entity_urns[1]

    # Verify no subResource fields for entity-level glossary terms
    assert "subResource" not in variables["input"]["resources"][0]
    assert "subResource" not in variables["input"]["resources"][1]


def test_add_glossary_terms_to_columns(mock_datahub_client):
    """Test adding glossary terms to specific columns (column-level)."""
    term_urns = ["urn:li:glossaryTerm:EmailAddress"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = ["email", "contact_email"]

    # Mock validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {
                    "urn": term_urns[0],
                    "type": "GLOSSARY_TERM",
                    "name": "EmailAddress",
                }
            ]
        },
        {"batchAddTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_glossary_terms(
            term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True
    assert "Successfully added 1 glossary term(s) to 2 entit(ies)" in result["message"]

    # Check the mutation call
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # Verify subResource fields are set for column-level glossary terms
    assert variables["input"]["resources"][0]["subResource"] == "email"
    assert variables["input"]["resources"][0]["subResourceType"] == "DATASET_FIELD"
    assert variables["input"]["resources"][1]["subResource"] == "contact_email"
    assert variables["input"]["resources"][1]["subResourceType"] == "DATASET_FIELD"


def test_add_glossary_terms_mixed_entity_and_column(mock_datahub_client):
    """Test adding glossary terms to both entity-level and column-level."""
    term_urns = ["urn:li:glossaryTerm:Revenue"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)",
    ]
    column_paths = [None, "total_amount"]  # Entity-level for first, column for second

    # Mock validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Revenue"}
            ]
        },
        {"batchAddTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_glossary_terms(
            term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # First resource should not have subResource (entity-level)
    assert "subResource" not in variables["input"]["resources"][0]
    # Second resource should have subResource (column-level)
    assert variables["input"]["resources"][1]["subResource"] == "total_amount"
    assert variables["input"]["resources"][1]["subResourceType"] == "DATASET_FIELD"


def test_add_glossary_terms_with_nonexistent_term(mock_datahub_client):
    """Test that adding nonexistent glossary term URN returns error."""
    term_urns = ["urn:li:glossaryTerm:NonExistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (term doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entities": []}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="do\ not\ exist\ in\ DataHub"):
            add_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_add_glossary_terms_with_non_term_urn(mock_datahub_client):
    """Test that adding a URN that is not a glossary term returns error."""
    term_urns = ["urn:li:tag:NotATerm"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a Tag entity instead of GlossaryTerm
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [
            {"urn": term_urns[0], "type": "TAG", "properties": {"name": "NotATerm"}}
        ]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not\ glossary\ term\ entities"):
            add_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_add_glossary_terms_mismatched_column_paths_length(mock_datahub_client):
    """Test that mismatched column_paths length raises ValueError."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test1", "urn:li:dataset:test2"]
    column_paths = ["column1"]  # Mismatched length: 1 vs 2

    # Mock successful validation
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="column_paths length"):
            add_glossary_terms(
                term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
            )


def test_add_glossary_terms_empty_term_urns(mock_datahub_client):
    """Test that empty term_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="term_urns cannot be empty"):
            add_glossary_terms(term_urns=[], entity_urns=["urn:li:dataset:test"])


def test_add_glossary_terms_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            add_glossary_terms(term_urns=["urn:li:glossaryTerm:Test"], entity_urns=[])


def test_add_glossary_terms_mutation_returns_false(mock_datahub_client):
    """Test handling of mutation returning false."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation failure
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]},
        {"batchAddTerms": False},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ add\ glossary\ terms"):
            add_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_add_glossary_terms_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution exception."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation raises exception
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]},
        Exception("GraphQL error"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error add\ glossary\ terms"):
            add_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


# ===== Tests for remove_glossary_terms =====


def test_remove_glossary_terms_from_multiple_datasets(mock_datahub_client):
    """Test removing glossary terms from multiple datasets (entity-level)."""
    term_urns = [
        "urn:li:glossaryTerm:Deprecated",
        "urn:li:glossaryTerm:LegacySystem",
    ]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)",
    ]

    # Mock validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Deprecated"},
                {"urn": term_urns[1], "type": "GLOSSARY_TERM", "name": "LegacySystem"},
            ]
        },
        {"batchRemoveTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)

    assert result["success"] is True
    assert (
        "Successfully removed 2 glossary term(s) from 2 entit(ies)" in result["message"]
    )

    # Verify GraphQL was called twice: once for validation, once for mutation
    assert mock_datahub_client._graph.execute_graphql.call_count == 2

    # Check the mutation call (second call)
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "batchRemoveTerms"
    variables = mutation_call.kwargs["variables"]

    assert variables["input"]["termUrns"] == term_urns
    assert len(variables["input"]["resources"]) == 2


def test_remove_glossary_terms_from_columns(mock_datahub_client):
    """Test removing glossary terms from specific columns (column-level)."""
    term_urns = ["urn:li:glossaryTerm:Confidential"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = ["old_ssn_field", "legacy_tax_id"]

    # Mock validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Confidential"}
            ]
        },
        {"batchRemoveTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_glossary_terms(
            term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # Verify subResource fields are set for column-level glossary terms
    assert variables["input"]["resources"][0]["subResource"] == "old_ssn_field"
    assert variables["input"]["resources"][0]["subResourceType"] == "DATASET_FIELD"
    assert variables["input"]["resources"][1]["subResource"] == "legacy_tax_id"
    assert variables["input"]["resources"][1]["subResourceType"] == "DATASET_FIELD"


def test_remove_glossary_terms_mixed_entity_and_column(mock_datahub_client):
    """Test removing glossary terms from both entity-level and column-level."""
    term_urns = ["urn:li:glossaryTerm:Experimental"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.production_table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
    ]
    column_paths = [None, "beta_feature"]  # Entity-level for first, column for second

    # Mock validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entities": [
                {"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Experimental"}
            ]
        },
        {"batchRemoveTerms": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_glossary_terms(
            term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
        )

    assert result["success"] is True

    # Check the mutation call
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    variables = mutation_call.kwargs["variables"]

    # First resource should not have subResource (entity-level)
    assert "subResource" not in variables["input"]["resources"][0]
    # Second resource should have subResource (column-level)
    assert variables["input"]["resources"][1]["subResource"] == "beta_feature"
    assert variables["input"]["resources"][1]["subResourceType"] == "DATASET_FIELD"


def test_remove_glossary_terms_with_nonexistent_term(mock_datahub_client):
    """Test that removing nonexistent glossary term URN returns error."""
    term_urns = ["urn:li:glossaryTerm:NonExistent"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning empty entities (term doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entities": []}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="do\ not\ exist\ in\ DataHub"):
            remove_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_remove_glossary_terms_with_non_term_urn(mock_datahub_client):
    """Test that removing a URN that is not a glossary term returns error."""
    term_urns = ["urn:li:tag:NotATerm"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation returning a Tag entity instead of GlossaryTerm
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [
            {"urn": term_urns[0], "type": "TAG", "properties": {"name": "NotATerm"}}
        ]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not\ glossary\ term\ entities"):
            remove_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_remove_glossary_terms_mismatched_column_paths_length(mock_datahub_client):
    """Test that mismatched column_paths length raises ValueError."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test1", "urn:li:dataset:test2"]
    column_paths = ["column1"]  # Mismatched length: 1 vs 2

    # Mock successful validation
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="column_paths length"):
            remove_glossary_terms(
                term_urns=term_urns, entity_urns=entity_urns, column_paths=column_paths
            )


def test_remove_glossary_terms_empty_term_urns(mock_datahub_client):
    """Test that empty term_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="term_urns cannot be empty"):
            remove_glossary_terms(term_urns=[], entity_urns=["urn:li:dataset:test"])


def test_remove_glossary_terms_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            remove_glossary_terms(
                term_urns=["urn:li:glossaryTerm:Test"], entity_urns=[]
            )


def test_remove_glossary_terms_mutation_returns_false(mock_datahub_client):
    """Test handling of mutation returning false."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation failure
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]},
        {"batchRemoveTerms": False},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ remove\ glossary\ terms"):
            remove_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)


def test_remove_glossary_terms_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution exception."""
    term_urns = ["urn:li:glossaryTerm:Test"]
    entity_urns = ["urn:li:dataset:test"]

    # Mock validation success, mutation raises exception
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entities": [{"urn": term_urns[0], "type": "GLOSSARY_TERM", "name": "Test"}]},
        Exception("GraphQL error"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error remove\ glossary\ terms"):
            remove_glossary_terms(term_urns=term_urns, entity_urns=entity_urns)
